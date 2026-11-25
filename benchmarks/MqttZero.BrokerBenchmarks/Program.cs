using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Transport.Sockets;
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
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Reflection;
using System.Text.Json;

Dictionary<string, string> arguments = ParseArguments(args);
string mode = GetValue(arguments, "mode", args.FirstOrDefault() ?? "compare");
if (GetBool(arguments, "diagnostics", false))
{
    AppContext.SetSwitch(
        MqttZero.Server.Broker.MqttZeroFanoutDiagnostics
            .AppContextSwitchName,
        true);
}
BenchmarkRuntimeConfiguration runtimeConfiguration =
    new(Math.Max(1, Environment.ProcessorCount));
BenchmarkRuntimeSnapshot runtimeSnapshot =
    runtimeConfiguration.ApplyToCurrentProcess();
BenchmarkTopologyOptions topologyOptions =
    BenchmarkTopologyOptions.Parse(arguments);
BenchmarkTopologyInfo topology = topologyOptions.CreateInfo();
BenchmarkEgressOptions egressOptions =
    BenchmarkEgressOptions.Parse(arguments);
BenchmarkEnvironmentInfo environment =
    BenchmarkObservationCapture.CaptureEnvironment(
        runtimeSnapshot,
        topology,
        egressOptions);

if (mode == "serve")
{
    await ServeAsync(arguments);
}
else if (mode == "load")
{
    int port = GetInt(arguments, "port", 1883);
    BenchmarkClientKind clientKind = BenchmarkClientSelection.Parse(
        GetValue(arguments, "client", "mqttnet"));
    BenchmarkRunOptions options = new(
        GetInt(arguments, "subscribers", 10),
        GetInt(arguments, "publishers", 1),
        GetInt(arguments, "qos", 0),
        GetInt(arguments, "payload", 1024),
        GetInt(arguments, "warmup", 2),
        GetInt(arguments, "seconds", 10),
        GetInt(arguments, "max-pending", 1024));
    BrokerBenchmarkResult result = await BenchmarkLoadRunner.RunAsync(
        port,
        options,
        new BenchmarkMqttClientFactory(clientKind, port),
        CancellationToken.None);
    await BenchmarkOutputWriter.WriteAsync(
        GetNullableValue(arguments, "output"),
        result,
        BrokerBenchmarkJsonContext.Default.BrokerBenchmarkResult,
        CancellationToken.None);
}
else if (mode == "compare")
{
    await CompareAsync(
        arguments,
        environment,
        runtimeConfiguration).ConfigureAwait(false);
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
    BenchmarkTopologyOptions topologyOptions =
        BenchmarkTopologyOptions.Parse(arguments);
    BenchmarkEgressOptions egressOptions =
        BenchmarkEgressOptions.Parse(arguments);
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
            options.EventLoopShardCount =
                topologyOptions.EventLoopShardCount;
            egressOptions.ApplyTo(options);
        });
        if (topologyOptions.IoQueueCount > 0)
        {
            builder.Services.Configure<SocketTransportOptions>(
                options => options.IOQueueCount =
                    topologyOptions.IoQueueCount);
        }
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

static async Task CompareAsync(
    Dictionary<string, string> arguments,
    BenchmarkEnvironmentInfo environment,
    BenchmarkRuntimeConfiguration runtimeConfiguration)
{
    string[] brokers = ["mqttzero", "mqttnet"];
    int[] qosLevels = GetIntArray(arguments, "qos", [0, 1, 2]);
    int[] subscriberCounts = GetIntArray(arguments, "subscribers", [1, 100, 500]);
    int[] payloadSizes = GetIntArray(arguments, "payload", [32, 8 * 1024, 64 * 1024]);
    int warmup = GetInt(arguments, "warmup", 15);
    int seconds = GetInt(arguments, "seconds", 60);
    int publishers = GetInt(arguments, "publishers", 1);
    int repetitions = GetInt(arguments, "repetitions", 1);
    int maxPendingMessages = GetInt(
        arguments,
        "max-pending",
        1024);
    BenchmarkTopologyOptions topologyOptions =
        BenchmarkTopologyOptions.Parse(arguments);
    BenchmarkEgressOptions egressOptions =
        BenchmarkEgressOptions.Parse(arguments);
    bool allowPacketFragmentation = GetBool(
        arguments,
        "fragmentation",
        true);
    BenchmarkClientKind clientKind = BenchmarkClientSelection.Parse(
        GetValue(arguments, "client", "mqttnet"));
    if (clientKind == BenchmarkClientKind.Raw &&
        qosLevels.Contains(2))
    {
        throw new ArgumentException(
            "The raw benchmark client supports QoS 0 and QoS 1 only.");
    }

    string? outputPath = GetNullableValue(arguments, "output");
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
                        long brokerStarted = Stopwatch.GetTimestamp();
                        using Process serverProcess = StartServerProcess(
                            assemblyPath,
                            broker,
                            port,
                            allowPacketFragmentation,
                            runtimeConfiguration,
                            topologyOptions,
                            egressOptions);
                        await WaitForReadyAsync(serverProcess);
                        BenchmarkProcessObservation brokerObservation;
                        try
                        {
                            BenchmarkRunOptions options = new(
                                subscribers,
                                publishers,
                                qos,
                                payloadSize,
                                warmup,
                                seconds,
                                maxPendingMessages);
                            BrokerBenchmarkResult result;
                            try
                            {
                                result = await BenchmarkLoadRunner.RunAsync(
                                    port,
                                    options,
                                    new BenchmarkMqttClientFactory(
                                        clientKind,
                                        port),
                                    CancellationToken.None);
                            }
                            catch (Exception exception)
                            {
                                throw new InvalidOperationException(
                                    $"Broker benchmark failed at " +
                                    $"repetition={repetition}, " +
                                    $"broker={broker}, " +
                                    $"qos={qos}, " +
                                    $"subscribers={subscribers}, " +
                                    $"payload={payloadSize}, port={port}.",
                                    exception);
                            }

                            brokerObservation =
                                BenchmarkObservationCapture.CaptureProcess(
                                    serverProcess,
                                    Stopwatch.GetElapsedTime(
                                        brokerStarted));
                            results.Add(new BrokerComparisonResult
                            {
                                Broker = broker,
                                Qos = qos,
                                Subscribers = subscribers,
                                PayloadSize = payloadSize,
                                Repetition = repetition,
                                Result = result,
                                BrokerObservation = brokerObservation
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

    BenchmarkGateResult gate = BenchmarkGateEvaluator.Evaluate(results);
    BenchmarkReport report = new(environment, results, gate);
    await BenchmarkOutputWriter.WriteAsync(
        outputPath,
        report,
        BrokerBenchmarkJsonContext.Default.BenchmarkReport,
        CancellationToken.None).ConfigureAwait(false);
    if (!gate.Passed)
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
    BenchmarkTopologyOptions topologyOptions =
        BenchmarkTopologyOptions.Parse(arguments);
    BenchmarkEgressOptions egressOptions =
        BenchmarkEgressOptions.Parse(arguments);
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
        options.EventLoopShardCount =
            topologyOptions.EventLoopShardCount;
        egressOptions.ApplyTo(options);
    });
    if (topologyOptions.IoQueueCount > 0)
    {
        builder.Services.Configure<SocketTransportOptions>(
            socketOptions => socketOptions.IOQueueCount =
                topologyOptions.IoQueueCount);
    }
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
    broker.ResetFanoutDiagnosticsForTest();
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
    MqttZero.Server.Broker.MqttZeroFanoutDiagnosticsSnapshot
        diagnosticsSnapshot = broker.GetFanoutDiagnosticsForTest();
    double averageFanoutMicroseconds =
        diagnosticsSnapshot.PublishCount == 0
            ? 0
            : diagnosticsSnapshot.TotalFanoutTicks *
                1_000_000d /
                Stopwatch.Frequency /
                diagnosticsSnapshot.PublishCount;
    double averageEgressSchedulerMicroseconds =
        diagnosticsSnapshot.EgressSchedulerPassCount == 0
            ? 0
            : diagnosticsSnapshot.TotalEgressSchedulerTicks *
                1_000_000d /
                Stopwatch.Frequency /
                diagnosticsSnapshot.EgressSchedulerPassCount;
    double averageEgressDrainMicroseconds =
        diagnosticsSnapshot.EgressDrainCount == 0
            ? 0
            : diagnosticsSnapshot.TotalEgressDrainTicks *
                1_000_000d /
                Stopwatch.Frequency /
                diagnosticsSnapshot.EgressDrainCount;
    double averageEgressBatchMessages =
        diagnosticsSnapshot.EgressDataBatchCount == 0
            ? 0
            : diagnosticsSnapshot.EgressBatchMessageCount /
                (double)diagnosticsSnapshot.EgressDataBatchCount;
    BenchmarkFanoutDiagnostics diagnostics = new(
        diagnosticsSnapshot.PublishCount,
        diagnosticsSnapshot.LocalBatchCount,
        diagnosticsSnapshot.RemoteBatchCount,
        diagnosticsSnapshot.DeliveryCount,
        averageFanoutMicroseconds,
        diagnosticsSnapshot.MaxFanoutTicks * 1_000_000d /
            Stopwatch.Frequency,
        diagnosticsSnapshot.EgressSignalCount,
        diagnosticsSnapshot.EgressDrainCount,
        diagnosticsSnapshot.EncodedPublishCount,
        diagnosticsSnapshot.FlushWaitCount,
        diagnosticsSnapshot.MaxPendingQueueDepth,
        diagnosticsSnapshot.EgressScheduledActorCount,
        diagnosticsSnapshot.EgressSchedulerPassCount,
        diagnosticsSnapshot.EgressScheduledActorPassCount,
        averageEgressSchedulerMicroseconds,
        averageEgressDrainMicroseconds,
        diagnosticsSnapshot.MaxEgressDrainTicks *
            1_000_000d /
            Stopwatch.Frequency,
        diagnosticsSnapshot.EgressDataBatchCount,
        diagnosticsSnapshot.EgressBatchMessageCount,
        diagnosticsSnapshot.EgressBatchByteCount,
        averageEgressBatchMessages,
        diagnosticsSnapshot.EgressFlushStartCount);
    OutboundBenchmarkResult result = new(
        PublishedMessages: loopResult.PublishedMessages,
        ReceivedMessages: Interlocked.Read(ref receivedMessages),
        FailedOperations: 0,
        ThroughputPerSecond:
            loopResult.PublishedMessages / stopwatch.Elapsed.TotalSeconds,
        WaitP50Microseconds: PercentileMicroseconds(
            waitTicks.ToArray(),
            waitTicks.Count,
            0.50),
        WaitP99Microseconds: PercentileMicroseconds(
            waitTicks.ToArray(),
            waitTicks.Count,
            0.99),
        PublishP50Microseconds: PercentileMicroseconds(
            publishTicks.ToArray(),
            publishTicks.Count,
            0.50),
        PublishP99Microseconds: PercentileMicroseconds(
            publishTicks.ToArray(),
            publishTicks.Count,
            0.99),
        FirstError: null,
        Diagnostics: diagnostics);
    Console.WriteLine(JsonSerializer.Serialize(
        result,
        BrokerBenchmarkJsonContext.Default.OutboundBenchmarkResult));
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
    bool allowPacketFragmentation,
    BenchmarkRuntimeConfiguration runtimeConfiguration,
    BenchmarkTopologyOptions topologyOptions,
    BenchmarkEgressOptions egressOptions)
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
    startInfo.ArgumentList.Add("--event-loop-shards");
    startInfo.ArgumentList.Add(
        topologyOptions.EventLoopShardCount.ToString(
            CultureInfo.InvariantCulture));
    startInfo.ArgumentList.Add("--io-queue-count");
    startInfo.ArgumentList.Add(
        topologyOptions.IoQueueCount.ToString(
            CultureInfo.InvariantCulture));
    startInfo.ArgumentList.Add("--outbound-batch-bytes");
    startInfo.ArgumentList.Add(
        egressOptions.OutboundBatchBytes.ToString(
            CultureInfo.InvariantCulture));
    startInfo.ArgumentList.Add("--outbound-batch-messages");
    startInfo.ArgumentList.Add(
        egressOptions.OutboundBatchMessages.ToString(
            CultureInfo.InvariantCulture));
    startInfo.ArgumentList.Add("--outbound-batch-duration-us");
    startInfo.ArgumentList.Add(
        egressOptions.OutboundBatchDurationMicroseconds.ToString(
            CultureInfo.InvariantCulture));
    foreach (KeyValuePair<string, string> variable in
        runtimeConfiguration.CreateBrokerEnvironmentVariables())
    {
        startInfo.Environment[variable.Key] = variable.Value;
    }

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

static MqttClientOptions CreateClientOptions(int port, string clientId)
{
    return new MqttClientOptionsBuilder()
        .WithTcpServer("127.0.0.1", port)
        .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
        .WithClientId(clientId)
        .WithCleanStart()
        .Build();
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

static string? GetNullableValue(
    IReadOnlyDictionary<string, string> arguments,
    string name)
{
    return arguments.TryGetValue(name, out string? value) ? value : null;
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
