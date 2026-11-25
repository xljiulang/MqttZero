using MqttZero.Protocol;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace MqttZero.BrokerBenchmarks;

internal static class BenchmarkLoadRunner
{
    public static async ValueTask<BrokerBenchmarkResult> RunAsync(
        int port,
        BenchmarkRunOptions options,
        IBenchmarkMqttClientFactory clientFactory,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(clientFactory);

        MqttQualityOfServiceLevel qualityOfServiceLevel = GetQos(options.Qos);
        List<IBenchmarkMqttClient> subscriberClients = [];
        List<IBenchmarkMqttClient> publisherClients = [];
        long receivedMessages = 0;
        long failedOperations = 0;
        long publishedMessages = 0;
        long receivedMessageBaseline = 0;
        ConcurrentQueue<string> errors = new();
        long setupStarted = Stopwatch.GetTimestamp();
        try
        {
            for (int index = 0; index < options.Subscribers; index++)
            {
                IBenchmarkMqttClient subscriber = clientFactory.Create();
                subscriberClients.Add(subscriber);
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
            byte[] payload = new byte[options.PayloadSize];
            bool ShouldPause()
            {
                return Interlocked.Read(ref publishedMessages) *
                        options.Subscribers >
                    Interlocked.Read(ref receivedMessages) +
                        options.MaxPendingMessages;
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

            long warmupStarted = Stopwatch.GetTimestamp();
            using CancellationTokenSource warmup = new(
                TimeSpan.FromSeconds(options.WarmupSeconds));
            await RunPublishersAsync(
                publisherClients,
                payload,
                qualityOfServiceLevel,
                ShouldPause,
                CountReceived,
                null,
                exception =>
                {
                    Interlocked.Increment(ref failedOperations);
                    errors.Enqueue(exception.ToString());
                },
                () => Interlocked.Increment(ref publishedMessages),
                warmup.Token)
                .ConfigureAwait(false);
            long warmupPublishingCompleted = Stopwatch.GetTimestamp();
            long warmupDrainStarted = Stopwatch.GetTimestamp();
            try
            {
                await WaitForSubscriberDrainAsync(
                    Interlocked.Read(ref publishedMessages) * options.Subscribers,
                    CountReceived,
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

            long[] latencySamples = new long[100_000];
            int latencySampleCount = 0;
            long measuredPublishes = 0;
            BenchmarkLoadSnapshot loadBeforeMeasurement =
                BenchmarkObservationCapture.CreateLoadSnapshot();
            Stopwatch measurement = Stopwatch.StartNew();
            using CancellationTokenSource measurementCancellation = new(
                TimeSpan.FromSeconds(options.MeasurementSeconds));
            await RunPublishersAsync(
                publisherClients,
                payload,
                qualityOfServiceLevel,
                ShouldPause,
                CountReceived,
                elapsedTicks =>
                {
                    int index = Interlocked.Increment(
                        ref latencySampleCount) - 1;
                    if (index < latencySamples.Length)
                    {
                        latencySamples[index] = elapsedTicks;
                    }
                },
                exception =>
                {
                    Interlocked.Increment(ref failedOperations);
                    errors.Enqueue(exception.ToString());
                },
                () => Interlocked.Increment(ref measuredPublishes),
                measurementCancellation.Token)
                .ConfigureAwait(false);
            measurement.Stop();
            BenchmarkLoadObservation loadObservation =
                BenchmarkObservationCapture.CaptureLoadDelta(
                    loadBeforeMeasurement);

            int sampleCount = Math.Min(
                latencySampleCount,
                latencySamples.Length);
            Array.Sort(latencySamples, 0, sampleCount);
            return new BrokerBenchmarkResult
            {
                PublishedMessages = measuredPublishes,
                ReceivedMessages = CountReceived(),
                FailedOperations = Interlocked.Read(ref failedOperations),
                WarmupPublishedMessages = warmupPublishedMessages,
                WarmupReceivedMessages = warmupReceivedMessages,
                ThroughputPerSecond =
                    measuredPublishes / measurement.Elapsed.TotalSeconds,
                LatencyP50Microseconds = PercentileMicroseconds(
                    latencySamples,
                    sampleCount,
                    0.50),
                LatencyP95Microseconds = PercentileMicroseconds(
                    latencySamples,
                    sampleCount,
                    0.95),
                LatencyP99Microseconds = PercentileMicroseconds(
                    latencySamples,
                    sampleCount,
                    0.99),
                LoadObservation = loadObservation,
                ConnectionSetupMilliseconds = Stopwatch.GetElapsedTime(
                    setupStarted,
                    connectionSetupCompleted).TotalMilliseconds,
                WarmupMilliseconds = Stopwatch.GetElapsedTime(
                    warmupStarted,
                    warmupPublishingCompleted).TotalMilliseconds,
                MeasurementMilliseconds = measurement.Elapsed.TotalMilliseconds,
                DrainMilliseconds = Stopwatch.GetElapsedTime(
                    warmupDrainStarted,
                    warmupDrainCompleted).TotalMilliseconds,
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
        byte[] payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        Func<bool> shouldPause,
        Func<long> getReceivedMessages,
        Action<long>? recordLatency,
        Action<Exception> recordFailure,
        Action recordPublish,
        CancellationToken cancellationToken)
    {
        Task[] tasks = new Task[publishers.Count];
        for (int index = 0; index < publishers.Count; index++)
        {
            IBenchmarkMqttClient publisher = publishers[index];
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

                        long started = Stopwatch.GetTimestamp();
                        try
                        {
                            await publisher.PublishAsync(
                                "bench/topic",
                                payload,
                                qualityOfServiceLevel,
                                CancellationToken.None).ConfigureAwait(false);
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

    private static async Task WaitForSubscriberDrainAsync(
        long advertisedMessages,
        Func<long> getReceivedMessages,
        CancellationToken cancellationToken)
    {
        using CancellationTokenSource timeout =
            CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));
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

            await client.DisposeAsync().ConfigureAwait(false);
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

    private static double PercentileMicroseconds(
        long[] samples,
        int count,
        double percentile)
    {
        if (count == 0)
        {
            return 0;
        }

        int index = Math.Clamp(
            (int)Math.Ceiling(percentile * count) - 1,
            0,
            count - 1);
        return samples[index] * 1_000_000d / Stopwatch.Frequency;
    }
}
