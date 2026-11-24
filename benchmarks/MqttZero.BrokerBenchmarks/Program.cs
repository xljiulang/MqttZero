using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MQTTnet.AspNetCore;
using MQTTnet.Protocol;
using MQTTnet.Server;
using MqttZero.AspNetCore;
using MqttZero.BrokerBenchmarks;
using MqttZero.Publishing;
using MqttZero.Server.Broker;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Reflection;
using System.Text.Json;

const double MinimumThroughputRatio = 1.0;
const double MaximumP99Ratio = 1.0;

Dictionary<string, string> arguments = ParseArguments(args);
string mode = GetValue(arguments, "mode", args.FirstOrDefault() ?? "compare");

if (mode == "serve")
{
    await ServeAsync(arguments);
}
else if (mode == "load")
{
    object result = await RunLoadAsync(
        GetInt(arguments, "port", 1883),
        GetInt(arguments, "subscribers", 10),
        GetInt(arguments, "publishers", 1),
        GetInt(arguments, "qos", 0),
        GetInt(arguments, "payload", 1024),
        GetInt(arguments, "warmup", 2),
        GetInt(arguments, "seconds", 10),
        GetInt(arguments, "max-pending", 1024));
    Console.WriteLine(JsonSerializer.Serialize(result));
}
else if (mode == "compare")
{
    await CompareAsync(arguments);
}
else if (mode == "outbound")
{
    await RunOutboundAsync(arguments);
}
else
{
    throw new ArgumentException($"Unknown mode '{mode}'.");
}

static async Task ServeAsync(Dictionary<string, string> arguments)
{
    string broker = GetValue(arguments, "broker", "mqttzero");
    int port = GetInt(arguments, "port", 1883);
    bool allowPacketFragmentation = GetBool(
        arguments,
        "fragmentation",
        true);
    CancellationTokenSource lifetime = new();
    Console.CancelKeyPress += (_, eventArgs) =>
    {
        eventArgs.Cancel = true;
        lifetime.Cancel();
    };

    if (broker == "mqttzero")
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.Listen(
                IPAddress.Loopback,
                port,
                listen => listen.UseMqttZero(options =>
                {
                    options.AllowPacketFragmentationSelector =
                        _ => allowPacketFragmentation;
                }));
        });
        builder.Services.AddMqttZero(options =>
        {
            options.MaxMessageQueueLength = 100_000;
        });
        await using WebApplication app = builder.Build();
        await app.StartAsync(lifetime.Token);
        Console.WriteLine("READY");
        await WaitForCancellationAsync(lifetime.Token);
        await app.StopAsync(CancellationToken.None);
        return;
    }

    if (broker == "mqttnet")
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddMqttServer(options =>
        {
            options
                .WithDefaultEndpoint()
                .WithDefaultEndpointBoundIPAddress(IPAddress.Loopback)
                .WithDefaultEndpointPort(port)
                .WithMaxPendingMessagesPerClient(100_000);
        });
        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.Listen(
                IPAddress.Loopback,
                port,
                listen => listen.UseMqtt());
        });
        await using WebApplication app = builder.Build();
        await app.StartAsync(lifetime.Token);
        Console.WriteLine("READY");
        await WaitForCancellationAsync(lifetime.Token);
        await app.StopAsync(CancellationToken.None);
        return;
    }

    throw new ArgumentException($"Unknown broker '{broker}'.");
}

static async Task CompareAsync(Dictionary<string, string> arguments)
{
    string[] brokers = ["mqttzero", "mqttnet"];
    int[] qosLevels = GetIntArray(arguments, "qos", [0, 1, 2]);
    int[] subscriberCounts = GetIntArray(arguments, "subscribers", [1, 100, 1000]);
    int[] payloadSizes = GetIntArray(arguments, "payload", [32, 8 * 1024, 64 * 1024]);
    int warmup = GetInt(arguments, "warmup", 15);
    int seconds = GetInt(arguments, "seconds", 60);
    int publishers = GetInt(arguments, "publishers", 1);
    int repetitions = GetInt(arguments, "repetitions", 1);
    int maxPendingMessages = GetInt(
        arguments,
        "max-pending",
        1024);
    bool allowPacketFragmentation = GetBool(
        arguments,
        "fragmentation",
        true);
    List<BrokerComparisonResult> results = [];
    string assemblyPath = Assembly.GetEntryAssembly()!.Location;
    int port = GetInt(arguments, "start-port", 19000);

    for (int repetition = 1; repetition <= repetitions; repetition++)
    {
        foreach (string broker in brokers)
        {
            foreach (int qos in qosLevels)
            {
                foreach (int subscribers in subscriberCounts)
                {
                    foreach (int payloadSize in payloadSizes)
                    {
                        port++;
                        using Process serverProcess = StartServerProcess(
                            assemblyPath,
                            broker,
                            port,
                            allowPacketFragmentation);
                        await WaitForReadyAsync(serverProcess);
                        try
                        {
                            BrokerBenchmarkResult result = await RunLoadAsync(
                                port,
                                subscribers,
                                publishers,
                                qos,
                                payloadSize,
                                warmup,
                                seconds,
                                maxPendingMessages);
                            results.Add(new BrokerComparisonResult
                            {
                                Broker = broker,
                                Qos = qos,
                                Subscribers = subscribers,
                                PayloadSize = payloadSize,
                                Repetition = repetition,
                                Result = result
                            });
                        }
                        finally
                        {
                            if (!serverProcess.HasExited)
                            {
                                serverProcess.Kill(entireProcessTree: true);
                            }
                        }
                    }
                }
            }
        }
    }

    IReadOnlyList<object> summaries = Summarize(results, out bool gatePassed);
    Console.WriteLine(JsonSerializer.Serialize(new
    {
        Results = results,
        Summary = summaries,
        GatePassed = gatePassed
    }, new JsonSerializerOptions
    {
        WriteIndented = true
    }));
    if (!gatePassed)
    {
        Environment.ExitCode = 1;
    }
}

static async Task RunOutboundAsync(Dictionary<string, string> arguments)
{
    int port = GetInt(arguments, "port", 19300);
    int qos = GetInt(arguments, "qos", 0);
    int subscriberCount = GetInt(arguments, "subscribers", 1);
    int payloadSize = GetInt(arguments, "payload", 8192);
    int warmupSeconds = GetInt(arguments, "warmup", 3);
    int measurementSeconds = GetInt(arguments, "seconds", 10);
    int maxPendingMessages = GetInt(
        arguments,
        "max-pending",
        1024);
    MqttZero.Protocol.MqttQualityOfServiceLevel qualityOfServiceLevel = qos switch
    {
        0 => MqttZero.Protocol.MqttQualityOfServiceLevel.AtMostOnce,
        1 => MqttZero.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
        2 => MqttZero.Protocol.MqttQualityOfServiceLevel.ExactlyOnce,
        _ => throw new InvalidOperationException(
            $"Unsupported QoS value: {qos}.")
    };
    WebApplicationBuilder builder = WebApplication.CreateBuilder();
    builder.Logging.ClearProviders();
    builder.WebHost.ConfigureKestrel(kestrel =>
    {
        kestrel.Listen(
            IPAddress.Loopback,
            port,
            listen => listen.UseMqttZero());
    });
    builder.Services.AddMqttZero(options =>
    {
        options.MaxMessageQueueLength = 100_000;
    });
    await using WebApplication app = builder.Build();
    await app.StartAsync();
    MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
    MqttClientFactory factory = new();
    List<IMqttClient> subscribers = [];
    long receivedMessages = 0;
    for (int index = 0; index < subscriberCount; index++)
    {
        IMqttClient subscriber = factory.CreateMqttClient();
        subscriber.ApplicationMessageReceivedAsync += _ =>
        {
            Interlocked.Increment(ref receivedMessages);
            return Task.CompletedTask;
        };
        subscribers.Add(subscriber);
        await subscriber.ConnectAsync(
            CreateClientOptions(port, $"outbound-subscriber-{index}"),
            CancellationToken.None);
        await subscriber.SubscribeAsync(
            new MqttClientSubscribeOptionsBuilder()
                .WithTopicFilter(
                    "bench/topic",
                    qos switch
                    {
                        0 => MqttQualityOfServiceLevel.AtMostOnce,
                        1 => MqttQualityOfServiceLevel.AtLeastOnce,
                        2 => MqttQualityOfServiceLevel.ExactlyOnce,
                        _ => throw new InvalidOperationException(
                            $"Unsupported QoS value: {qos}.")
                    })
                .Build(),
            CancellationToken.None);
    }
    MqttZeroPublishMessage message = new()
    {
        Topic = "bench/topic",
        Payload = new ReadOnlySequence<byte>(new byte[payloadSize]),
        QualityOfServiceLevel = qualityOfServiceLevel
    };
    using CancellationTokenSource warmup = new(
        TimeSpan.FromSeconds(warmupSeconds));
    OutboundLoopResult warmupResult = await RunOutboundPublishLoopAsync(
        broker,
        message,
        maxPendingMessages,
        warmup.Token,
        subscriberCount,
        () => Interlocked.Read(ref receivedMessages),
        collectTimings: false);
    await WaitForSubscriberDrainAsync(
        advertisedMessages:
            warmupResult.PublishedMessages * subscriberCount,
        () => Interlocked.Read(ref receivedMessages),
        CancellationToken.None);
    Interlocked.Exchange(ref receivedMessages, 0);
    Stopwatch stopwatch = Stopwatch.StartNew();
    List<long> waitTicks = [];
    List<long> publishTicks = [];
    using CancellationTokenSource measurement = new(
        TimeSpan.FromSeconds(measurementSeconds));
    OutboundLoopResult loopResult = await RunOutboundPublishLoopAsync(
        broker,
        message,
        maxPendingMessages,
        measurement.Token,
        subscriberCount,
        () => Interlocked.Read(ref receivedMessages),
        collectTimings: true,
        waitTicks,
        publishTicks);
    stopwatch.Stop();
    waitTicks.Sort();
    publishTicks.Sort();
    Console.WriteLine(JsonSerializer.Serialize(new
    {
        PublishedMessages = loopResult.PublishedMessages,
        ReceivedMessages = Interlocked.Read(ref receivedMessages),
        FailedOperations = 0,
        ThroughputPerSecond =
            loopResult.PublishedMessages / stopwatch.Elapsed.TotalSeconds,
        WaitP50Microseconds = PercentileMicroseconds(
            waitTicks.ToArray(),
            waitTicks.Count,
            0.50),
        WaitP99Microseconds = PercentileMicroseconds(
            waitTicks.ToArray(),
            waitTicks.Count,
            0.99),
        PublishP50Microseconds = PercentileMicroseconds(
            publishTicks.ToArray(),
            publishTicks.Count,
            0.50),
        PublishP99Microseconds = PercentileMicroseconds(
            publishTicks.ToArray(),
            publishTicks.Count,
            0.99),
        FirstError = (string?)null
    }));
    foreach (IMqttClient subscriber in subscribers)
    {
        await subscriber.DisconnectAsync();
        subscriber.Dispose();
    }

    await app.StopAsync();
}

static async Task<OutboundLoopResult> RunOutboundPublishLoopAsync(
    MqttZeroBroker broker,
    MqttZeroPublishMessage message,
    int maxPendingMessages,
    CancellationToken cancellationToken,
    int deliveryMultiplier,
    Func<long> getReceivedMessages,
    bool collectTimings,
    List<long>? waitTicks = null,
    List<long>? publishTicks = null)
{
    long publishedMessages = 0;
    try
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            long waitStarted = Stopwatch.GetTimestamp();
            while (publishedMessages * deliveryMultiplier -
                getReceivedMessages() >
                maxPendingMessages)
            {
                await Task.Delay(
                    TimeSpan.FromMilliseconds(1),
                    cancellationToken);
            }

            if (collectTimings)
            {
                waitTicks!.Add(Stopwatch.GetTimestamp() - waitStarted);
            }

            long publishStarted = Stopwatch.GetTimestamp();
            await broker.PublishAsync(message, cancellationToken);
            if (collectTimings)
            {
                publishTicks!.Add(Stopwatch.GetTimestamp() - publishStarted);
            }

            publishedMessages++;
        }
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
    }

    return new OutboundLoopResult(publishedMessages);
}

static Process StartServerProcess(
    string assemblyPath,
    string broker,
    int port,
    bool allowPacketFragmentation)
{
    ProcessStartInfo startInfo = new()
    {
        FileName = "dotnet",
        RedirectStandardOutput = true,
        RedirectStandardError = true,
        UseShellExecute = false,
        CreateNoWindow = true
    };
    startInfo.ArgumentList.Add(assemblyPath);
    startInfo.ArgumentList.Add("serve");
    startInfo.ArgumentList.Add("--broker");
    startInfo.ArgumentList.Add(broker);
    startInfo.ArgumentList.Add("--port");
    startInfo.ArgumentList.Add(port.ToString(CultureInfo.InvariantCulture));
    startInfo.ArgumentList.Add("--fragmentation");
    startInfo.ArgumentList.Add(
        allowPacketFragmentation ? "true" : "false");
    return Process.Start(startInfo) ??
        throw new InvalidOperationException("Could not start the broker process.");
}

static async Task WaitForReadyAsync(Process process)
{
    using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(20));
    string? line = await process.StandardOutput.ReadLineAsync(timeout.Token);
    if (line == "READY")
    {
        return;
    }

    string error = await process.StandardError.ReadToEndAsync(timeout.Token);
    throw new InvalidOperationException(
        $"Broker process did not become ready. Output: {line}. Error: {error}");
}

static async Task<BrokerBenchmarkResult> RunLoadAsync(
    int port,
    int subscribers,
    int publishers,
    int qos,
    int payloadSize,
    int warmupSeconds,
    int measurementSeconds,
    int maxPendingMessages)
{
    MqttClientFactory factory = new();
    MqttQualityOfServiceLevel qualityOfServiceLevel = qos switch
    {
        0 => MqttQualityOfServiceLevel.AtMostOnce,
        1 => MqttQualityOfServiceLevel.AtLeastOnce,
        2 => MqttQualityOfServiceLevel.ExactlyOnce,
        _ => throw new InvalidOperationException(
            $"Unsupported QoS value: {qos}.")
    };
    List<IMqttClient> subscriberClients = [];
    List<IMqttClient> publisherClients = [];
    long receivedMessages = 0;
    long failedOperations = 0;
    long publishedMessages = 0;
    ConcurrentQueue<string> errors = new();
    try
    {
        for (int index = 0; index < subscribers; index++)
        {
            IMqttClient subscriber = factory.CreateMqttClient();
            subscriber.ApplicationMessageReceivedAsync += _ =>
            {
                Interlocked.Increment(ref receivedMessages);
                return Task.CompletedTask;
            };
            subscriberClients.Add(subscriber);
            await subscriber.ConnectAsync(
                CreateClientOptions(port, $"subscriber-{index}"),
                CancellationToken.None);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter("bench/topic", qualityOfServiceLevel)
                    .Build(),
                CancellationToken.None);
        }

        for (int index = 0; index < publishers; index++)
        {
            IMqttClient publisher = factory.CreateMqttClient();
            publisherClients.Add(publisher);
            await publisher.ConnectAsync(
                CreateClientOptions(port, $"publisher-{index}"),
                CancellationToken.None);
        }

        byte[] payload = new byte[payloadSize];
        MqttApplicationMessage message = new MqttApplicationMessageBuilder()
            .WithTopic("bench/topic")
            .WithPayload(payload)
            .WithQualityOfServiceLevel(qualityOfServiceLevel)
            .Build();
        bool ShouldPause()
        {
            return Interlocked.Read(ref publishedMessages) * subscribers >
                Interlocked.Read(ref receivedMessages) + maxPendingMessages;
        }

        using CancellationTokenSource warmup = new(TimeSpan.FromSeconds(warmupSeconds));
        await RunPublishersAsync(
            publisherClients,
            message,
            warmup.Token,
            ShouldPause,
            null,
            exception =>
            {
                Interlocked.Increment(ref failedOperations);
                errors.Enqueue(exception.ToString());
            },
            () => Interlocked.Increment(ref publishedMessages));
        await WaitForSubscriberDrainAsync(
            advertisedMessages:
                Interlocked.Read(ref publishedMessages) * subscribers,
            () => Interlocked.Read(ref receivedMessages),
            CancellationToken.None);
        long warmupPublishedMessages =
            Interlocked.Read(ref publishedMessages);
        long warmupReceivedMessages =
            Interlocked.Read(ref receivedMessages);
        Interlocked.Exchange(ref receivedMessages, 0);
        Interlocked.Exchange(ref publishedMessages, 0);

        long[] latencySamples = new long[100_000];
        int latencySampleCount = 0;
        long measuredPublishes = 0;
        Stopwatch stopwatch = Stopwatch.StartNew();
        using CancellationTokenSource measurement = new(TimeSpan.FromSeconds(measurementSeconds));
        await RunPublishersAsync(
            publisherClients,
            message,
            measurement.Token,
            ShouldPause,
            elapsedTicks =>
            {
                int index = Interlocked.Increment(ref latencySampleCount) - 1;
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
            () =>
            {
                Interlocked.Increment(ref measuredPublishes);
                Interlocked.Increment(ref publishedMessages);
            });
        stopwatch.Stop();

        Array.Sort(latencySamples, 0, Math.Min(latencySampleCount, latencySamples.Length));
        int sampleCount = Math.Min(latencySampleCount, latencySamples.Length);
        return new BrokerBenchmarkResult
        {
            PublishedMessages = measuredPublishes,
            ReceivedMessages = Interlocked.Read(ref receivedMessages),
            FailedOperations = Interlocked.Read(ref failedOperations),
            WarmupPublishedMessages = warmupPublishedMessages,
            WarmupReceivedMessages = warmupReceivedMessages,
            ThroughputPerSecond = measuredPublishes / stopwatch.Elapsed.TotalSeconds,
            LatencyP50Microseconds = PercentileMicroseconds(latencySamples, sampleCount, 0.50),
            LatencyP95Microseconds = PercentileMicroseconds(latencySamples, sampleCount, 0.95),
            LatencyP99Microseconds = PercentileMicroseconds(latencySamples, sampleCount, 0.99),
            FirstError = errors.TryPeek(out string? firstError) ? firstError : null
        };
    }
    finally
    {
        foreach (IMqttClient client in publisherClients.Concat(subscriberClients))
        {
            if (client.IsConnected)
            {
                await client.DisconnectAsync();
            }

            client.Dispose();
        }
    }
}

static async Task WaitForSubscriberDrainAsync(
    long advertisedMessages,
    Func<long> getReceivedMessages,
    CancellationToken cancellationToken)
{
    using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(
        cancellationToken);
    timeout.CancelAfter(TimeSpan.FromSeconds(30));
    while (getReceivedMessages() < advertisedMessages)
    {
        await Task.Delay(TimeSpan.FromMilliseconds(1), timeout.Token);
    }
}

static IReadOnlyList<object> Summarize(
    IReadOnlyList<BrokerComparisonResult> results,
    out bool gatePassed)
{
    gatePassed = true;
    List<object> summaries = [];
    foreach (IGrouping<(int Qos, int Subscribers, int PayloadSize), BrokerComparisonResult> group in
        results.GroupBy(static result => (
            result.Qos,
            result.Subscribers,
            result.PayloadSize)))
    {
        BrokerComparisonResult[] mqttZero = group
            .Where(static result => result.Broker == "mqttzero")
            .ToArray();
        BrokerComparisonResult[] mqttNet = group
            .Where(static result => result.Broker == "mqttnet")
            .ToArray();
        if (mqttZero.Length == 0 || mqttNet.Length == 0)
        {
            continue;
        }

        double mqttZeroThroughput = Median(
            mqttZero.Select(static result => result.Result.ThroughputPerSecond));
        double mqttNetThroughput = Median(
            mqttNet.Select(static result => result.Result.ThroughputPerSecond));
        double mqttZeroP99 = Median(
            mqttZero.Select(static result => result.Result.LatencyP99Microseconds));
        double mqttNetP99 = Median(
            mqttNet.Select(static result => result.Result.LatencyP99Microseconds));
        double throughputRatio = mqttZeroThroughput / mqttNetThroughput;
        double p99Ratio = mqttZeroP99 / mqttNetP99;
        bool cellPassed =
            throughputRatio >= MinimumThroughputRatio &&
            p99Ratio <= MaximumP99Ratio;
        gatePassed &= cellPassed;
        summaries.Add(new
        {
            group.Key.Qos,
            group.Key.Subscribers,
            group.Key.PayloadSize,
            MqttZeroThroughput = mqttZeroThroughput,
            MqttNetThroughput = mqttNetThroughput,
            ThroughputRatio = throughputRatio,
            MqttZeroP99Microseconds = mqttZeroP99,
            MqttNetP99Microseconds = mqttNetP99,
            P99Ratio = p99Ratio,
            GatePassed = cellPassed
        });
    }

    return summaries;
}

static double Median(IEnumerable<double> values)
{
    double[] ordered = values.Order().ToArray();
    if (ordered.Length == 0)
    {
        return 0;
    }

    int middle = ordered.Length / 2;
    return ordered.Length % 2 == 0
        ? (ordered[middle - 1] + ordered[middle]) / 2
        : ordered[middle];
}

static MqttClientOptions CreateClientOptions(int port, string clientId)
{
    return new MqttClientOptionsBuilder()
        .WithTcpServer("127.0.0.1", port)
        .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
        .WithClientId(clientId)
        .WithCleanStart()
        .Build();
}

static async Task RunPublishersAsync(
    IReadOnlyList<IMqttClient> publishers,
    MqttApplicationMessage message,
    CancellationToken cancellationToken,
    Func<bool>? shouldPause,
    Action<long>? recordLatency,
    Action<Exception> recordFailure,
    Action? recordPublish = null)
{
    Task[] tasks = new Task[publishers.Count];
    for (int index = 0; index < publishers.Count; index++)
    {
        IMqttClient publisher = publishers[index];
        tasks[index] = Task.Run(
            async () =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    while (shouldPause?.Invoke() == true)
                    {
                        await Task.Delay(
                            TimeSpan.FromMilliseconds(1),
                            cancellationToken);
                    }

                    long started = Stopwatch.GetTimestamp();
                    try
                    {
                        await publisher.PublishAsync(message, CancellationToken.None);
                        recordLatency?.Invoke(Stopwatch.GetTimestamp() - started);
                        recordPublish?.Invoke();
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
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
        await Task.WhenAll(tasks);
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
    }
}

static double PercentileMicroseconds(long[] samples, int count, double percentile)
{
    if (count == 0)
    {
        return 0;
    }

    int index = Math.Clamp((int)Math.Ceiling(percentile * count) - 1, 0, count - 1);
    return samples[index] * 1_000_000d / Stopwatch.Frequency;
}

static async Task WaitForCancellationAsync(CancellationToken cancellationToken)
{
    try
    {
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
    }
    catch (OperationCanceledException)
    {
    }
}

static Dictionary<string, string> ParseArguments(string[] args)
{
    Dictionary<string, string> result = new(StringComparer.OrdinalIgnoreCase);
    if (args.Length > 0 && !args[0].StartsWith("--", StringComparison.Ordinal))
    {
        result["mode"] = args[0];
    }

    for (int index = 0; index < args.Length - 1; index++)
    {
        if (args[index].StartsWith("--", StringComparison.Ordinal))
        {
            result[args[index][2..]] = args[index + 1];
            index++;
        }
    }

    return result;
}

static string GetValue(
    IReadOnlyDictionary<string, string> arguments,
    string name,
    string defaultValue)
{
    return arguments.TryGetValue(name, out string? value) ? value : defaultValue;
}

static int GetInt(
    IReadOnlyDictionary<string, string> arguments,
    string name,
    int defaultValue)
{
    return arguments.TryGetValue(name, out string? value)
        ? int.Parse(value, CultureInfo.InvariantCulture)
        : defaultValue;
}

static bool GetBool(
    IReadOnlyDictionary<string, string> arguments,
    string name,
    bool defaultValue)
{
    return arguments.TryGetValue(name, out string? value)
        ? bool.Parse(value)
        : defaultValue;
}

static int[] GetIntArray(
    IReadOnlyDictionary<string, string> arguments,
    string name,
    int[] defaultValue)
{
    if (!arguments.TryGetValue(name, out string? value))
    {
        return defaultValue;
    }

    return value.Split(
        ',',
        StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Select(item => int.Parse(item, CultureInfo.InvariantCulture))
        .ToArray();
}
