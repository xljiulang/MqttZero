using MqttZero.Protocol;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace MqttZero.BrokerBenchmarks;

internal static class BenchmarkLoadRunner
{
    internal static bool ShouldPause(
        long publishedMessages,
        long receivedMessages,
        int subscribers,
        int maxPendingMessages)
    {
        long advertisedDeliveries = checked(
            publishedMessages * subscribers);
        return advertisedDeliveries >
            receivedMessages + maxPendingMessages;
    }

    public static async ValueTask<BrokerBenchmarkResult> RunAsync(
        int port,
        BenchmarkRunOptions options,
        IBenchmarkMqttClientFactory clientFactory,
        CancellationToken cancellationToken,
        Action<BenchmarkRunProgress>? progress = null,
        TimeSpan? drainTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(clientFactory);
        ArgumentOutOfRangeException.ThrowIfNegative(
            options.MessageExpirySeconds);
        ArgumentOutOfRangeException.ThrowIfNegative(
            options.SubscriberDelayMicroseconds);
        TimeSpan effectiveDrainTimeout =
            drainTimeout ?? TimeSpan.FromSeconds(30);
        uint? messageExpiryInterval =
            options.MessageExpirySeconds > 0
                ? (uint)options.MessageExpirySeconds
                : null;

        MqttQualityOfServiceLevel qualityOfServiceLevel = GetQos(options.Qos);
        List<IBenchmarkMqttClient> subscriberClients = [];
        List<IBenchmarkMqttClient> publisherClients = [];
        List<BenchmarkDeliveryTracker> subscriberTrackers = [];
        long receivedMessages = 0;
        long failedOperations = 0;
        long publishedMessages = 0;
        long publishSequence = 0;
        long receivedMessageBaseline = 0;
        ConcurrentQueue<string> errors = new();
        long setupStarted = Stopwatch.GetTimestamp();
        try
        {
            for (int index = 0; index < options.Subscribers; index++)
            {
                IBenchmarkMqttClient subscriber = clientFactory.Create();
                subscriberClients.Add(subscriber);
                subscriberTrackers.Add(subscriber.Delivery);
                await subscriber.ConnectAsync(
                    $"subscriber-{index}",
                    cancellationToken).ConfigureAwait(false);
                await subscriber.SubscribeAsync(
                    "bench/topic",
                    qualityOfServiceLevel,
                    cancellationToken).ConfigureAwait(false);
            }

            for (int index = 0; index < options.Publishers; index++)
            {
                IBenchmarkMqttClient publisher = clientFactory.Create();
                publisherClients.Add(publisher);
                await publisher.ConnectAsync(
                    $"publisher-{index}",
                    cancellationToken).ConfigureAwait(false);
            }

            long connectionSetupCompleted = Stopwatch.GetTimestamp();
            byte[][] payloads = new byte[Math.Max(1, options.Publishers)][];
            for (int index = 0; index < payloads.Length; index++)
            {
                payloads[index] = new byte[options.PayloadSize];
            }

            long NextSequence()
            {
                return Interlocked.Increment(ref publishSequence);
            }

            bool ShouldPause(long publishedMessages)
            {
                return BenchmarkLoadRunner.ShouldPause(
                    publishedMessages,
                    Interlocked.Read(ref receivedMessages),
                    options.Subscribers,
                    options.MaxPendingMessages);
            }

            long CountReceived()
            {
                long total = 0;
                for (int index = 0; index < subscriberClients.Count; index++)
                {
                    total += subscriberClients[index].ReceivedMessageCount;
                }

                long current = Math.Max(
                    0,
                    total - Interlocked.Read(ref receivedMessageBaseline));
                Interlocked.Exchange(ref receivedMessages, current);
                return current;
            }

            void ReportProgress(
                string stage,
                long publishedMessages)
            {
                progress?.Invoke(new BenchmarkRunProgress(
                    stage,
                    publishedMessages,
                    CountReceived(),
                    Interlocked.Read(ref failedOperations)));
            }

            long warmupStarted = Stopwatch.GetTimestamp();
            ReportProgress("connected", 0);
            using CancellationTokenSource warmup = new(
                TimeSpan.FromSeconds(options.WarmupSeconds));
            await RunPublishersAsync(
                publisherClients,
                payloads,
                qualityOfServiceLevel,
                () => ShouldPause(
                    Interlocked.Read(ref publishedMessages)),
                CountReceived,
                null,
                exception =>
                {
                    Interlocked.Increment(ref failedOperations);
                    errors.Enqueue(exception.ToString());
                },
                () => Interlocked.Increment(ref publishedMessages),
                NextSequence,
                messageExpiryInterval,
                warmup.Token)
                .ConfigureAwait(false);
            long warmupPublishingCompleted = Stopwatch.GetTimestamp();
            long warmupDrainStarted = Stopwatch.GetTimestamp();
            try
            {
                await WaitForSubscriberDrainAsync(
                    Interlocked.Read(ref publishedMessages) * options.Subscribers,
                    CountReceived,
                    effectiveDrainTimeout,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (!cancellationToken.IsCancellationRequested)
            {
                long advertised =
                    Interlocked.Read(ref publishedMessages) *
                    options.Subscribers;
                throw new TimeoutException(
                    $"Subscriber warmup drain timed out. " +
                    $"Advertised={advertised}, " +
                    $"Received={CountReceived()}, " +
                    $"Published={Interlocked.Read(ref publishedMessages)}, " +
                    $"Subscribers={options.Subscribers}, " +
                    $"Qos={options.Qos}, " +
                    $"Payload={options.PayloadSize}.");
            }
            long warmupDrainCompleted = Stopwatch.GetTimestamp();
            long warmupPublishedMessages =
                Interlocked.Read(ref publishedMessages);
            long warmupReceivedMessages = CountReceived();
            Interlocked.Exchange(
                ref receivedMessageBaseline,
                warmupReceivedMessages);
            Interlocked.Exchange(ref publishedMessages, 0);
            ReportProgress("warmup-complete", warmupPublishedMessages);

            using MqttZeroLatencyHistogram latencyHistogram = new();
            using MqttZeroLatencyHistogram deliveryHistogram = new();
            foreach (BenchmarkDeliveryTracker tracker in subscriberTrackers)
            {
                tracker.AttachLatency(deliveryHistogram);
            }

            long measuredPublishes = 0;
            BenchmarkLoadSnapshot loadBeforeMeasurement =
                BenchmarkObservationCapture.CreateLoadSnapshot();
            ReportProgress("measurement-start", 0);
            long measurementStarted = Stopwatch.GetTimestamp();
            Stopwatch measurement = Stopwatch.StartNew();
            using CancellationTokenSource measurementCancellation = new(
                TimeSpan.FromSeconds(options.MeasurementSeconds));
            using CancellationTokenSource progressCancellation = new();
            Task progressTask = progress == null
                ? Task.CompletedTask
                : MonitorMeasurementProgressAsync(
                    progress,
                    () => Interlocked.Read(ref measuredPublishes),
                    CountReceived,
                    () => Interlocked.Read(ref failedOperations),
                    progressCancellation.Token);
            try
            {
                await RunPublishersAsync(
                    publisherClients,
                    payloads,
                    qualityOfServiceLevel,
                    () => ShouldPause(
                        Interlocked.Read(ref measuredPublishes)),
                    CountReceived,
                    latencyHistogram.RecordTicks,
                    exception =>
                    {
                        Interlocked.Increment(ref failedOperations);
                        errors.Enqueue(exception.ToString());
                    },
                    () => Interlocked.Increment(ref measuredPublishes),
                    NextSequence,
                    messageExpiryInterval,
                    measurementCancellation.Token)
                    .ConfigureAwait(false);
            }
            finally
            {
                foreach (BenchmarkDeliveryTracker tracker in subscriberTrackers)
                {
                    tracker.AttachLatency(null);
                }

                progressCancellation.Cancel();
                try
                {
                    await progressTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                    when (progressCancellation.IsCancellationRequested)
                {
                }
            }

            measurement.Stop();
            long measurementReceived = CountReceived();
            ReportProgress(
                "measurement-complete",
                Interlocked.Read(ref measuredPublishes));
            long measurementDrainStarted = Stopwatch.GetTimestamp();
            try
            {
                await WaitForSubscriberDrainAsync(
                    Interlocked.Read(ref measuredPublishes) *
                        options.Subscribers,
                    CountReceived,
                    effectiveDrainTimeout,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (!cancellationToken.IsCancellationRequested)
            {
                long advertised =
                    Interlocked.Read(ref measuredPublishes) *
                    options.Subscribers;
                throw new TimeoutException(
                    $"Subscriber measurement drain timed out. " +
                    $"Advertised={advertised}, " +
                    $"Received={CountReceived()}, " +
                    $"Published={Interlocked.Read(ref measuredPublishes)}, " +
                    $"Subscribers={options.Subscribers}, " +
                    $"Qos={options.Qos}, " +
                    $"Payload={options.PayloadSize}.");
            }

            long measurementDrainCompleted = Stopwatch.GetTimestamp();
            long measurementReceivedCompleted = CountReceived();
            ReportProgress(
                "measurement-drained",
                Interlocked.Read(ref measuredPublishes));
            double endToEndSeconds = Stopwatch.GetElapsedTime(
                measurementStarted,
                measurementDrainCompleted).TotalSeconds;
            BenchmarkLoadObservation loadObservation =
                BenchmarkObservationCapture.CaptureLoadDelta(
                    loadBeforeMeasurement);
            long totalPublishedMessages =
                warmupPublishedMessages + measuredPublishes;
            long minimumMaxSequence = long.MaxValue;
            long sequenceRegressions = 0;
            long duplicateDeliveries = 0;
            long unsequencedDeliveries = 0;
            for (int index = 0; index < subscriberTrackers.Count; index++)
            {
                BenchmarkDeliveryTracker tracker = subscriberTrackers[index];
                minimumMaxSequence = Math.Min(
                    minimumMaxSequence,
                    tracker.MaxSequence);
                sequenceRegressions += tracker.SequenceRegressions;
                duplicateDeliveries += Math.Max(
                    0,
                    tracker.ReceivedMessages - tracker.MaxSequence);
                unsequencedDeliveries += tracker.UnsequencedMessages;
            }

            if (subscriberTrackers.Count == 0)
            {
                minimumMaxSequence = totalPublishedMessages;
            }

            return new BrokerBenchmarkResult
            {
                PublishedMessages = measuredPublishes,
                Publishers = options.Publishers,
                ReceivedMessages = measurementReceived,
                MeasurementReceivedMessages =
                    measurementReceivedCompleted,
                FailedOperations = Interlocked.Read(ref failedOperations),
                WarmupPublishedMessages = warmupPublishedMessages,
                WarmupReceivedMessages = warmupReceivedMessages,
                PublishWindowThroughputPerSecond =
                    measuredPublishes / measurement.Elapsed.TotalSeconds,
                EndToEndThroughputPerSecond =
                    endToEndSeconds > 0
                        ? measuredPublishes / endToEndSeconds
                        : 0,
                DrainRatio = endToEndSeconds > 0
                    ? Stopwatch.GetElapsedTime(
                        measurementDrainStarted,
                        measurementDrainCompleted).TotalSeconds /
                        endToEndSeconds
                    : 0,
                PublishCallLatencyP50Microseconds =
                    latencyHistogram.GetPercentileMicroseconds(0.50),
                PublishCallLatencyP95Microseconds =
                    latencyHistogram.GetPercentileMicroseconds(0.95),
                PublishCallLatencyP99Microseconds =
                    latencyHistogram.GetPercentileMicroseconds(0.99),
                PublishCallLatencyMaxMicroseconds =
                    latencyHistogram.MaxMicroseconds,
                DeliveryLatencyP50Microseconds =
                    deliveryHistogram.GetPercentileMicroseconds(0.50),
                DeliveryLatencyP95Microseconds =
                    deliveryHistogram.GetPercentileMicroseconds(0.95),
                DeliveryLatencyP99Microseconds =
                    deliveryHistogram.GetPercentileMicroseconds(0.99),
                DeliveryLatencyMaxMicroseconds =
                    deliveryHistogram.MaxMicroseconds,
                SequenceRegressions = sequenceRegressions,
                DuplicateDeliveryMessages = duplicateDeliveries,
                MissingDeliveryMessages = Math.Max(
                    0,
                    totalPublishedMessages - minimumMaxSequence),
                UnsequencedDeliveryMessages = unsequencedDeliveries,
                LoadObservation = loadObservation,
                ConnectionSetupMilliseconds = Stopwatch.GetElapsedTime(
                    setupStarted,
                    connectionSetupCompleted).TotalMilliseconds,
                WarmupMilliseconds = Stopwatch.GetElapsedTime(
                    warmupStarted,
                    warmupPublishingCompleted).TotalMilliseconds,
                MeasurementMilliseconds = measurement.Elapsed.TotalMilliseconds,
                WarmupDrainMilliseconds = Stopwatch.GetElapsedTime(
                    warmupDrainStarted,
                    warmupDrainCompleted).TotalMilliseconds,
                MeasurementDrainMilliseconds = Stopwatch.GetElapsedTime(
                    measurementDrainStarted,
                    measurementDrainCompleted).TotalMilliseconds,
                FirstError = errors.TryPeek(out string? firstError)
                    ? firstError
                    : null
            };
        }
        finally
        {
            await DisposeClientsAsync(
                publisherClients,
                subscriberClients).ConfigureAwait(false);
        }
    }

    private static async ValueTask RunPublishersAsync(
        List<IBenchmarkMqttClient> publishers,
        byte[][] payloads,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        Func<bool> shouldPause,
        Func<long> getReceivedMessages,
        Action<long>? recordLatency,
        Action<Exception> recordFailure,
        Action recordPublish,
        Func<long> nextSequence,
        uint? messageExpiryInterval,
        CancellationToken cancellationToken)
    {
        Task[] tasks = new Task[publishers.Count];
        for (int index = 0; index < publishers.Count; index++)
        {
            IBenchmarkMqttClient publisher = publishers[index];
            byte[] payload = payloads[index % payloads.Length];
            tasks[index] = Task.Run(
                async () =>
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        while (!cancellationToken.IsCancellationRequested &&
                            shouldPause())
                        {
                            _ = getReceivedMessages();
                            await Task.Delay(
                                TimeSpan.FromMilliseconds(1),
                                cancellationToken).ConfigureAwait(false);
                        }

                        BenchmarkDeliveryEnvelope.Write(
                            payload,
                            nextSequence(),
                            Stopwatch.GetTimestamp());
                        long started = Stopwatch.GetTimestamp();
                        try
                        {
                            await publisher.PublishAsync(
                                "bench/topic",
                                payload,
                                qualityOfServiceLevel,
                                CancellationToken.None,
                                messageExpiryInterval).ConfigureAwait(false);
                            recordLatency?.Invoke(
                                Stopwatch.GetTimestamp() - started);
                            recordPublish();
                        }
                        catch (OperationCanceledException)
                            when (cancellationToken.IsCancellationRequested)
                        {
                            return;
                        }
                        catch (Exception exception)
                        {
                            recordFailure(exception);
                        }
                    }
                },
                cancellationToken);
        }

        try
        {
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private static async Task MonitorMeasurementProgressAsync(
        Action<BenchmarkRunProgress> progress,
        Func<long> getPublishedMessages,
        Func<long> getReceivedMessages,
        Func<long> getFailedOperations,
        CancellationToken cancellationToken)
    {
        using PeriodicTimer timer = new(TimeSpan.FromSeconds(5));
        while (await timer.WaitForNextTickAsync(cancellationToken)
            .ConfigureAwait(false))
        {
            progress(new BenchmarkRunProgress(
                "measurement",
                getPublishedMessages(),
                getReceivedMessages(),
                getFailedOperations()));
        }
    }

    private static async Task WaitForSubscriberDrainAsync(
        long advertisedMessages,
        Func<long> getReceivedMessages,
        TimeSpan drainTimeout,
        CancellationToken cancellationToken)
    {
        using CancellationTokenSource timeout =
            CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken);
        timeout.CancelAfter(drainTimeout);
        while (getReceivedMessages() < advertisedMessages)
        {
            await Task.Delay(
                TimeSpan.FromMilliseconds(1),
                timeout.Token).ConfigureAwait(false);
        }
    }

    private static async ValueTask DisposeClientsAsync(
        IReadOnlyList<IBenchmarkMqttClient> publishers,
        IReadOnlyList<IBenchmarkMqttClient> subscribers)
    {
        foreach (IBenchmarkMqttClient client in publishers.Concat(subscribers))
        {
            try
            {
                if (client.IsConnected)
                {
                    using CancellationTokenSource disconnectTimeout = new(
                        TimeSpan.FromSeconds(5));
                    await client.DisconnectAsync(
                        disconnectTimeout.Token).ConfigureAwait(false);
                }
            }
            catch (Exception)
            {
            }

            try
            {
                await client.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
        }
    }

    private static MqttQualityOfServiceLevel GetQos(int qos)
    {
        return qos switch
        {
            0 => MqttQualityOfServiceLevel.AtMostOnce,
            1 => MqttQualityOfServiceLevel.AtLeastOnce,
            2 => MqttQualityOfServiceLevel.ExactlyOnce,
            _ => throw new InvalidOperationException(
                $"Unsupported QoS value: {qos}.")
        };
    }

}
