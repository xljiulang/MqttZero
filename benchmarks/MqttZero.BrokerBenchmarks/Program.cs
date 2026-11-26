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
        GetInt(arguments, "max-pending", 1024),
        GetInt(arguments, "message-expiry", 0),
        GetInt(arguments, "subscriber-delay-us", 0));
    EnsureClientSupportsExpiry(clientKind, options);
    TimeSpan drainTimeout = GetDrainTimeout(arguments);
    BrokerBenchmarkResult result = await BenchmarkLoadRunner.RunAsync(
        port,
        options,
        new BenchmarkMqttClientFactory(
            clientKind,
            port,
            receiveDelay: GetReceiveDelay(options)),
        CancellationToken.None,
        update => WriteProgress(
            1,
            1,
            clientKind.ToString(),
            options.Qos,
            options.Subscribers,
            options.PayloadSize,
            update),
        drainTimeout);
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
else if (mode == "memory")
{
    await RunMemoryAsync(arguments);
}
else if (mode == "ab")
{
    await RunAbAsync(
        arguments,
        environment,
        runtimeConfiguration).ConfigureAwait(false);
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
    long maxBufferedBytes = GetLong(
        arguments,
        "max-buffered-bytes",
        0);
    int memoryProbeMilliseconds = GetInt(
        arguments,
        "memory-probe-ms",
        0);
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
            if (maxBufferedBytes > 0)
            {
                options.MaxBufferedBytes = maxBufferedBytes;
            }

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
        if (memoryProbeMilliseconds > 0)
        {
            MqttZeroBroker probeBroker =
                app.Services.GetRequiredService<MqttZeroBroker>();
            BrokerMemoryProbeState probeState = new();
            _ = Task.Run(
                () => RunMemoryProbeAsync(
                    probeBroker,
                    probeState,
                    memoryProbeMilliseconds,
                    lifetime.Token),
                CancellationToken.None);
            _ = Task.Run(
                () => ReadProbeCommandsAsync(
                    probeState,
                    lifetime.Token),
                CancellationToken.None);
        }

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
    int messageExpirySeconds = GetInt(
        arguments,
        "message-expiry",
        0);
    int subscriberDelayMicroseconds = GetInt(
        arguments,
        "subscriber-delay-us",
        0);
    TimeSpan drainTimeout = GetDrainTimeout(arguments);
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
    bool enforceRankingComparison = GetBool(
        arguments,
        "comparison-gate",
        true);
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
    int totalRuns =
        repetitions *
        brokers.Length *
        qosLevels.Length *
        subscriberCounts.Length *
        payloadSizes.Length;
    int currentRun = 0;

    async Task<BrokerComparisonResult> RunOneAsync(
        int runPort,
        int repetition,
        string broker,
        int qos,
        int subscribers,
        int payloadSize,
        int runNumber,
        int runTotal)
    {
        long brokerStarted = Stopwatch.GetTimestamp();
        using Process serverProcess = StartServerProcess(
            assemblyPath,
            broker,
            runPort,
            allowPacketFragmentation,
            runtimeConfiguration,
            topologyOptions,
            egressOptions);
        try
        {
            await WaitForReadyAsync(serverProcess);
            BenchmarkRunOptions options = new(
                subscribers,
                publishers,
                qos,
                payloadSize,
                warmup,
                seconds,
                maxPendingMessages,
                messageExpirySeconds,
                subscriberDelayMicroseconds);
            EnsureClientSupportsExpiry(clientKind, options);
            BrokerBenchmarkResult result;
            try
            {
                result = await BenchmarkLoadRunner.RunAsync(
                    runPort,
                    options,
                    new BenchmarkMqttClientFactory(
                        clientKind,
                        runPort,
                        receiveDelay: GetReceiveDelay(options)),
                    CancellationToken.None,
                    update => WriteProgress(
                        runNumber,
                        runTotal,
                        broker,
                        qos,
                        subscribers,
                        payloadSize,
                        update),
                    drainTimeout);
            }
            catch (Exception exception)
            {
                throw new InvalidOperationException(
                    $"Broker benchmark failed at " +
                    $"repetition={repetition}, " +
                    $"broker={broker}, qos={qos}, " +
                    $"subscribers={subscribers}, " +
                    $"payload={payloadSize}, port={runPort}.",
                    exception);
            }

            BenchmarkProcessObservation brokerObservation =
                BenchmarkObservationCapture.CaptureProcess(
                    serverProcess,
                    Stopwatch.GetElapsedTime(brokerStarted));
            BrokerComparisonResult comparison = new()
            {
                Broker = broker,
                Qos = qos,
                Subscribers = subscribers,
                PayloadSize = payloadSize,
                Repetition = repetition,
                Result = result,
                BrokerObservation = brokerObservation
            };
            WriteProgress(
                runNumber,
                runTotal,
                broker,
                qos,
                subscribers,
                payloadSize,
                new BenchmarkRunProgress(
                    "complete",
                    result.PublishedMessages,
                    result.MeasurementReceivedMessages,
                    result.FailedOperations),
                result);
            return comparison;
        }
        finally
        {
            if (!serverProcess.HasExited)
            {
                serverProcess.Kill(entireProcessTree: true);
            }
        }
    }

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
                        currentRun++;
                        WriteProgress(
                            currentRun,
                            totalRuns,
                            broker,
                            qos,
                            subscribers,
                            payloadSize,
                            new BenchmarkRunProgress(
                                "starting",
                                0,
                                0,
                                0));
                        results.Add(await RunOneAsync(
                            port,
                            repetition,
                            broker,
                            qos,
                            subscribers,
                            payloadSize,
                            currentRun,
                            totalRuns));
                    }
                }
            }
        }
    }

    if (repetitions == 2)
    {
        foreach (IGrouping<
            (int Qos, int Subscribers, int PayloadSize),
            BrokerComparisonResult> cell in results
                .GroupBy(static result => (
                    result.Qos,
                    result.Subscribers,
                    result.PayloadSize))
                .ToArray())
        {
            if (!BenchmarkRepetitionPolicy.RequiresThirdRepetition(
                    cell.ToArray()))
            {
                continue;
            }

            totalRuns += brokers.Length;
            foreach (string broker in brokers)
            {
                port++;
                currentRun++;
                WriteProgress(
                    currentRun,
                    totalRuns,
                    broker,
                    cell.Key.Qos,
                    cell.Key.Subscribers,
                    cell.Key.PayloadSize,
                    new BenchmarkRunProgress(
                        "tie-break",
                        0,
                        0,
                        0));
                results.Add(await RunOneAsync(
                    port,
                    repetition: 3,
                    broker,
                    cell.Key.Qos,
                    cell.Key.Subscribers,
                    cell.Key.PayloadSize,
                    currentRun,
                    totalRuns));
            }
        }
    }

    BenchmarkGateResult gate = BenchmarkGateEvaluator.Evaluate(
        results,
        enforceRankingComparison);
    BenchmarkReport report = new(environment, results, gate);
    await BenchmarkOutputWriter.WriteAsync(
        outputPath,
        report,
        BrokerBenchmarkJsonContext.Default.BenchmarkReport,
        CancellationToken.None).ConfigureAwait(false);
    Console.WriteLine(
        $"compare gate passed={gate.Passed} " +
        $"ranking-enforced={enforceRankingComparison} " +
        $"cells={gate.Summaries.Count}");
    foreach (string error in gate.Errors)
    {
        Console.Error.WriteLine($"compare gate failure: {error}");
    }

    if (!gate.Passed)
    {
        Environment.ExitCode = 1;
    }
}

static async Task RunAbAsync(
    Dictionary<string, string> arguments,
    BenchmarkEnvironmentInfo environment,
    BenchmarkRuntimeConfiguration runtimeConfiguration)
{
    string baselineAssembly = GetValue(
        arguments,
        "baseline-broker",
        string.Empty);
    string candidateAssembly = GetValue(
        arguments,
        "candidate-broker",
        string.Empty);
    if (baselineAssembly.Length == 0 || candidateAssembly.Length == 0)
    {
        throw new ArgumentException(
            "The ab mode requires --baseline-broker and --candidate-broker.");
    }

    Dictionary<string, string> baselineArguments =
        BenchmarkVariantArguments.Build(arguments, "baseline");
    Dictionary<string, string> candidateArguments =
        BenchmarkVariantArguments.Build(arguments, "candidate");
    BrokerAbVariantInfo baseline = new(
        "baseline",
        Path.GetFullPath(baselineAssembly),
        GetNullableValue(arguments, "baseline-commit"),
        BenchmarkTopologyOptions.Parse(baselineArguments),
        BenchmarkEgressOptions.Parse(baselineArguments));
    BrokerAbVariantInfo candidate = new(
        "candidate",
        Path.GetFullPath(candidateAssembly),
        GetNullableValue(arguments, "candidate-commit"),
        BenchmarkTopologyOptions.Parse(candidateArguments),
        BenchmarkEgressOptions.Parse(candidateArguments));
    int qos = GetInt(arguments, "qos", 1);
    int subscribers = GetInt(arguments, "subscribers", 100);
    int payloadSize = GetInt(arguments, "payload", 64 * 1024);
    int publishers = GetInt(arguments, "publishers", 1);
    int warmup = GetInt(arguments, "warmup", 5);
    int seconds = GetInt(arguments, "seconds", 20);
    int rounds = Math.Max(1, GetInt(arguments, "rounds", 3));
    int maxPendingMessages = GetInt(arguments, "max-pending", 1024);
    TimeSpan drainTimeout = GetDrainTimeout(arguments);
    int startPort = GetInt(arguments, "start-port", 21000);
    int minimumGainPercent = GetInt(
        arguments,
        "min-gain-percent",
        10);
    int maximumP99RegressionPercent = GetInt(
        arguments,
        "max-delivery-p99-regression-percent",
        5);
    bool allowPacketFragmentation = GetBool(
        arguments,
        "fragmentation",
        true);
    if (payloadSize < BenchmarkDeliveryEnvelope.HeaderLength)
    {
        throw new ArgumentException(
            "The benchmark payload must hold the delivery envelope.");
    }

    BenchmarkClientKind clientKind = BenchmarkClientSelection.Parse(
        GetValue(arguments, "client", "mqttnet"));
    if (clientKind == BenchmarkClientKind.Raw &&
        qos == 2)
    {
        throw new ArgumentException(
            "The raw benchmark client supports QoS 0 and QoS 1 only.");
    }

    List<BrokerAbRoundResult> results = [];
    int port = startPort;
    for (int round = 1; round <= rounds; round++)
    {
        BrokerAbVariantInfo[] order = round % 2 == 1
            ? [baseline, candidate]
            : [candidate, baseline];
        foreach (BrokerAbVariantInfo variant in order)
        {
            port++;
            Console.Error.WriteLine(
                FormattableString.Invariant(
                    $"ab round={round}/{rounds} variant={variant.Name} qos={qos} subscribers={subscribers} payload={payloadSize} port={port}"));
            using Process serverProcess = StartServerProcess(
                variant.AssemblyPath,
                "mqttzero",
                port,
                allowPacketFragmentation,
                runtimeConfiguration,
                variant.Topology!,
                variant.Egress!);
            long started = Stopwatch.GetTimestamp();
            try
            {
                await WaitForReadyAsync(serverProcess);
                BenchmarkRunOptions options = new(
                    subscribers,
                    publishers,
                    qos,
                    payloadSize,
                    warmup,
                    seconds,
                    maxPendingMessages,
                    GetInt(arguments, "message-expiry", 0),
                    GetInt(arguments, "subscriber-delay-us", 0));
                EnsureClientSupportsExpiry(clientKind, options);
                BrokerBenchmarkResult result =
                    await BenchmarkLoadRunner.RunAsync(
                        port,
                        options,
                        new BenchmarkMqttClientFactory(
                            clientKind,
                            port,
                            receiveDelay: GetReceiveDelay(options)),
                        CancellationToken.None,
                        progress: null,
                        drainTimeout: drainTimeout).ConfigureAwait(false);
                BenchmarkProcessObservation brokerObservation =
                    BenchmarkObservationCapture.CaptureProcess(
                        serverProcess,
                        Stopwatch.GetElapsedTime(started));
                using Process clientProcess =
                    Process.GetCurrentProcess();
                BenchmarkProcessObservation clientObservation =
                    BenchmarkObservationCapture.CaptureProcess(
                        clientProcess,
                        Stopwatch.GetElapsedTime(started));
                results.Add(new BrokerAbRoundResult
                {
                    Round = round,
                    Variant = variant.Name,
                    Commit = variant.Commit,
                    Result = result,
                    BrokerObservation = brokerObservation,
                    ClientObservation = clientObservation
                });
                Console.Error.WriteLine(
                    FormattableString.Invariant(
                        $"ab round={round}/{rounds} variant={variant.Name} end-to-end={result.EndToEndThroughputPerSecond:F2} msg/s publish-p99={result.PublishCallLatencyP99Microseconds:F1} us delivery-p99={result.DeliveryLatencyP99Microseconds:F1} us drain={result.MeasurementDrainMilliseconds:F1} ms failed={result.FailedOperations} missing={result.MissingDeliveryMessages} duplicates={result.DuplicateDeliveryMessages}"));
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

    BrokerAbSummary summary = BrokerAbEvaluator.Evaluate(
        baseline,
        candidate,
        results,
        minimumGainPercent / 100d,
        maximumP99RegressionPercent / 100d);
    BrokerAbReport report = new(
        environment,
        baseline,
        candidate,
        qos,
        subscribers,
        payloadSize,
        results,
        summary);
    await BenchmarkOutputWriter.WriteAsync(
        GetNullableValue(arguments, "output"),
        report,
        BrokerBenchmarkJsonContext.Default.BrokerAbReport,
        CancellationToken.None).ConfigureAwait(false);
    Console.Error.WriteLine(
        FormattableString.Invariant(
            $"ab gate passed={summary.Passed} throughput-change={summary.ThroughputChangeRatio:P1} delivery-p99-ratio={summary.DeliveryP99Ratio:F3} reason={summary.FailureReason ?? "none"}"));
    if (!summary.Passed)
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
    long expectedReceivedMessages =
        loopResult.PublishedMessages * subscriberCount;
    long measurementDrainStarted = Stopwatch.GetTimestamp();
    await WaitForSubscriberDrainAsync(
        advertisedMessages: expectedReceivedMessages,
        () => Interlocked.Read(ref receivedMessages),
        CancellationToken.None);
    long measurementDrainCompleted = Stopwatch.GetTimestamp();
    TimeSpan measurementDrain = Stopwatch.GetElapsedTime(
        measurementDrainStarted,
        measurementDrainCompleted);
    double endToEndSeconds =
        stopwatch.Elapsed.TotalSeconds +
        measurementDrain.TotalSeconds;
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
        diagnosticsSnapshot.EgressFlushStartCount,
        diagnosticsSnapshot.SessionLockWaitCount,
        ToAverageMicroseconds(
            diagnosticsSnapshot.TotalSessionLockWaitTicks,
            diagnosticsSnapshot.SessionLockWaitCount),
        ToAverageMicroseconds(
            diagnosticsSnapshot.MaxSessionLockWaitTicks,
            1),
        ToAverageMicroseconds(
            diagnosticsSnapshot.TotalPendingSelectionTicks,
            diagnosticsSnapshot.EgressDataBatchCount),
        ToAverageMicroseconds(
            diagnosticsSnapshot.TotalInflightUpdateTicks,
            diagnosticsSnapshot.EncodedPublishCount),
        ToAverageMicroseconds(
            diagnosticsSnapshot.TotalPublishEncodeTicks,
            diagnosticsSnapshot.EncodedPublishCount),
        ToAverageMicroseconds(
            diagnosticsSnapshot.TotalPublishWriteTicks,
            diagnosticsSnapshot.EgressDataBatchCount));
    OutboundBenchmarkResult result = new(
        PublishedMessages: loopResult.PublishedMessages,
        ReceivedMessages: Interlocked.Read(ref receivedMessages),
        ExpectedReceivedMessages: expectedReceivedMessages,
        FailedOperations: 0,
        ThroughputPerSecond:
            loopResult.PublishedMessages / stopwatch.Elapsed.TotalSeconds,
        EndToEndThroughputPerSecond:
            endToEndSeconds > 0
                ? loopResult.PublishedMessages / endToEndSeconds
                : 0,
        MeasurementDrainMilliseconds:
            measurementDrain.TotalMilliseconds,
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

static async Task RunMemoryAsync(Dictionary<string, string> arguments)
{
    string? brokerAssembly = GetNullableValue(
        arguments,
        "broker-assembly");
    if (!string.IsNullOrWhiteSpace(brokerAssembly))
    {
        await RunExternalMemoryAsync(
            brokerAssembly,
            arguments).ConfigureAwait(false);
        return;
    }

    await RunInProcessMemoryAsync(arguments).ConfigureAwait(false);
}

static async Task RunInProcessMemoryAsync(
    Dictionary<string, string> arguments)
{
    int port = GetInt(arguments, "port", 19400);
    int qos = GetInt(arguments, "qos", 0);
    int subscriberCount = GetInt(arguments, "subscribers", 1);
    int payloadSize = GetInt(arguments, "payload", 8192);
    int messagesPerWave = GetInt(arguments, "messages", 100);
    int waveCount = GetInt(arguments, "waves", 3);
    long bufferQuota = GetLong(
        arguments,
        "buffer-bytes",
        512L * 1024 * 1024);
    int maxPendingMessages = GetInt(
        arguments,
        "max-pending",
        1024);
    BenchmarkTopologyOptions topologyOptions =
        BenchmarkTopologyOptions.Parse(arguments);
    BenchmarkEgressOptions egressOptions =
        BenchmarkEgressOptions.Parse(arguments);
    MqttZero.Protocol.MqttQualityOfServiceLevel qualityOfServiceLevel =
        qos switch
        {
            0 => MqttZero.Protocol.MqttQualityOfServiceLevel.AtMostOnce,
            1 => MqttZero.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
            2 => MqttZero.Protocol.MqttQualityOfServiceLevel.ExactlyOnce,
            _ => throw new InvalidOperationException(
                $"Unsupported QoS value: {qos}.")
        };
    MQTTnet.Protocol.MqttQualityOfServiceLevel mqttnetQos = qos switch
    {
        0 => MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce,
        1 => MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
        2 => MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce,
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
        options.MaxBufferedBytes = bufferQuota;
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
            CreateClientOptions(port, $"memory-subscriber-{index}"),
            CancellationToken.None);
        await subscriber.SubscribeAsync(
            new MqttClientSubscribeOptionsBuilder()
                .WithTopicFilter("bench/memory", mqttnetQos)
                .Build(),
            CancellationToken.None);
    }

    MqttZeroPublishMessage message = new()
    {
        Topic = "bench/memory",
        Payload = new ReadOnlySequence<byte>(new byte[payloadSize]),
        QualityOfServiceLevel = qualityOfServiceLevel
    };
    using (CancellationTokenSource warmup = new(
        TimeSpan.FromSeconds(30)))
    {
        long warmupMessages = Math.Max(1, messagesPerWave / 10);
        await RunFixedPublishLoopAsync(
                broker,
                message,
                maxPendingMessages,
                subscriberCount,
                () => Interlocked.Read(ref receivedMessages),
                warmupMessages,
                warmup.Token);
        await WaitForSubscriberDrainAsync(
            warmupMessages * subscriberCount,
            () => Interlocked.Read(ref receivedMessages),
            CancellationToken.None);
    }

    ForceGarbageCollection();
    List<MemoryWaveResult> waves = [];
    for (int wave = 1; wave <= waveCount; wave++)
    {
        Interlocked.Exchange(ref receivedMessages, 0);
        await RunFixedPublishLoopAsync(
                broker,
                message,
                maxPendingMessages,
                subscriberCount,
                () => Interlocked.Read(ref receivedMessages),
                messagesPerWave,
                CancellationToken.None);
        long expected =
            messagesPerWave * subscriberCount;
        long drainStarted = Stopwatch.GetTimestamp();
        await WaitForSubscriberDrainAsync(
            expected,
            () => Interlocked.Read(ref receivedMessages),
            CancellationToken.None);
        TimeSpan drain = Stopwatch.GetElapsedTime(drainStarted);
        ForceGarbageCollection();
        MqttZeroBrokerMemorySnapshot brokerMemory =
            broker.GetMemorySnapshotForTest();
        MemoryWaveResult result = new(
            wave,
            messagesPerWave,
            Interlocked.Read(ref receivedMessages),
            0,
            drain.TotalMilliseconds,
            BenchmarkMemorySnapshot.Capture(brokerMemory));
        waves.Add(result);
        Console.Error.WriteLine(
            FormattableString.Invariant(
                $"memory-wave={wave} published={result.PublishedMessages} received={result.ReceivedMessages}/{expected} buffer-committed={result.Snapshot.BufferCommittedBytes} route-retained={result.Snapshot.RoutePlanRetainedBytes} shared-cursors={result.Snapshot.SharedCursorCount}"));
    }

    MemoryBenchmarkResult benchmark = new(
        waves.ToArray(),
        new MemoryRunInfo(
            "in-process",
            null,
            null,
            bufferQuota,
            0,
            qos,
            subscriberCount,
            payloadSize,
            messagesPerWave,
            waves.Count > 0
                ? waves.Max(static wave => wave.Snapshot.BufferCommittedBytes)
                : 0,
            waves.Count > 0
                ? waves.Max(static wave => wave.Snapshot.ManagedHeapBytes)
                : 0,
            waves.Count > 0
                ? waves.Max(static wave => wave.Snapshot.WorkingSetBytes)
                : 0));
    await BenchmarkOutputWriter.WriteAsync(
        GetNullableValue(arguments, "output"),
        benchmark,
        BrokerBenchmarkJsonContext.Default.MemoryBenchmarkResult,
        CancellationToken.None);
    foreach (IMqttClient subscriber in subscribers)
    {
        await subscriber.DisconnectAsync();
        subscriber.Dispose();
    }

    await app.StopAsync();
}

static async Task RunExternalMemoryAsync(
    string brokerAssembly,
    Dictionary<string, string> arguments)
{
    int port = GetInt(arguments, "port", 19410);
    int qos = GetInt(arguments, "qos", 0);
    int subscriberCount = GetInt(arguments, "subscribers", 1);
    int payloadSize = GetInt(arguments, "payload", 8192);
    int messagesPerWave = GetInt(arguments, "messages", 100);
    int waveCount = GetInt(arguments, "waves", 3);
    int maxPendingMessages = GetInt(
        arguments,
        "max-pending",
        1024);
    long bufferQuota = GetLong(
        arguments,
        "buffer-bytes",
        512L * 1024 * 1024);
    int probeMilliseconds = Math.Max(
        10,
        GetInt(arguments, "memory-probe-ms", 100));
    string? brokerCommit = GetNullableValue(arguments, "broker-commit");
    BenchmarkTopologyOptions topologyOptions =
        BenchmarkTopologyOptions.Parse(arguments);
    BenchmarkEgressOptions egressOptions =
        BenchmarkEgressOptions.Parse(arguments);
    BenchmarkRuntimeConfiguration brokerRuntime =
        new(Math.Max(1, Environment.ProcessorCount));
    MQTTnet.Protocol.MqttQualityOfServiceLevel mqttnetQos = qos switch
    {
        0 => MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce,
        1 => MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
        2 => MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce,
        _ => throw new InvalidOperationException(
            $"Unsupported QoS value: {qos}.")
    };
    string assemblyPath = Path.GetFullPath(brokerAssembly);
    using Process serverProcess = StartServerProcess(
        assemblyPath,
        "mqttzero",
        port,
        true,
        brokerRuntime,
        topologyOptions,
        egressOptions,
        bufferQuota,
        probeMilliseconds);
    BenchmarkMemoryProbeCollector collector = new();
    using CancellationTokenSource probeLifetime = new();
    Task probeTask = ReadProbeLinesAsync(
        serverProcess.StandardError,
        collector,
        probeLifetime.Token);
    MqttClientFactory factory = new();
    List<IMqttClient> subscribers = [];
    IMqttClient? publisher = null;
    long receivedMessages = 0;
    BenchmarkMemorySnapshot? idleSnapshot = null;
    try
    {
        await WaitForReadyAsync(serverProcess);
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
                CreateClientOptions(port, $"memory-subscriber-{index}"),
                CancellationToken.None);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter("bench/memory", mqttnetQos)
                    .Build(),
                CancellationToken.None);
        }

        publisher = factory.CreateMqttClient();
        await publisher.ConnectAsync(
            CreateClientOptions(port, "memory-publisher"),
            CancellationToken.None);
        BrokerMemoryProbeSnapshot idleProbe =
            await WaitForFreshProbeAsync(
                serverProcess,
                collector,
                probeMilliseconds,
                CancellationToken.None).ConfigureAwait(false);
        idleSnapshot = BenchmarkMemorySnapshot.FromProbe(idleProbe);

        byte[] payload = new byte[payloadSize];
        using (CancellationTokenSource warmup = new(
            TimeSpan.FromSeconds(30)))
        {
            long warmupMessages = Math.Max(1, messagesPerWave / 10);
            await RunTcpPublishLoopAsync(
                    publisher,
                    payload,
                    mqttnetQos,
                    maxPendingMessages,
                    subscriberCount,
                    () => Interlocked.Read(ref receivedMessages),
                    warmupMessages,
                    warmup.Token)
                .ConfigureAwait(false);
            await WaitForSubscriberDrainAsync(
                warmupMessages * subscriberCount,
                () => Interlocked.Read(ref receivedMessages),
                CancellationToken.None).ConfigureAwait(false);
        }

        List<MemoryWaveResult> waves = [];
        for (int wave = 1; wave <= waveCount; wave++)
        {
            Interlocked.Exchange(ref receivedMessages, 0);
            collector.ResetPeak();
            await RunTcpPublishLoopAsync(
                    publisher,
                    payload,
                    mqttnetQos,
                    maxPendingMessages,
                    subscriberCount,
                    () => Interlocked.Read(ref receivedMessages),
                    messagesPerWave,
                    CancellationToken.None)
                .ConfigureAwait(false);
            long expected = messagesPerWave * subscriberCount;
            long drainStarted = Stopwatch.GetTimestamp();
            await WaitForSubscriberDrainAsync(
                expected,
                () => Interlocked.Read(ref receivedMessages),
                CancellationToken.None).ConfigureAwait(false);
            double drainMilliseconds = Stopwatch.GetElapsedTime(
                drainStarted).TotalMilliseconds;
            BrokerMemoryProbeSnapshot latest =
                await WaitForFreshProbeAsync(
                    serverProcess,
                    collector,
                    probeMilliseconds,
                    CancellationToken.None).ConfigureAwait(false);
            MemoryWaveResult result = new(
                wave,
                messagesPerWave,
                Interlocked.Read(ref receivedMessages),
                0,
                drainMilliseconds,
                BenchmarkMemorySnapshot.FromProbe(latest),
                collector.Peak);
            waves.Add(result);
            Console.Error.WriteLine(
                FormattableString.Invariant(
                    $"memory-wave={wave} published={result.PublishedMessages} received={result.ReceivedMessages}/{expected} committed-peak={result.Peak!.BufferCommittedBytes} route-in-use={result.Snapshot.RoutePlanInUseCount} heap={result.Snapshot.ManagedHeapBytes}"));
        }

        MemoryBenchmarkResult benchmark = new(
            waves.ToArray(),
            new MemoryRunInfo(
                "tcp",
                assemblyPath,
                brokerCommit,
                bufferQuota,
                probeMilliseconds,
                qos,
                subscriberCount,
                payloadSize,
                messagesPerWave,
                waves.Max(static wave =>
                    wave.Peak?.BufferCommittedBytes ?? 0),
                waves.Max(static wave =>
                    wave.Peak?.ManagedHeapBytes ?? 0),
                waves.Max(static wave =>
                    wave.Peak?.WorkingSetBytes ?? 0),
                idleSnapshot));
        await BenchmarkOutputWriter.WriteAsync(
            GetNullableValue(arguments, "output"),
            benchmark,
            BrokerBenchmarkJsonContext.Default.MemoryBenchmarkResult,
            CancellationToken.None).ConfigureAwait(false);
    }
    finally
    {
        if (publisher != null)
        {
            await publisher.DisconnectAsync().ConfigureAwait(false);
            publisher.Dispose();
        }

        foreach (IMqttClient subscriber in subscribers)
        {
            await subscriber.DisconnectAsync().ConfigureAwait(false);
            subscriber.Dispose();
        }

        await probeLifetime.CancelAsync().ConfigureAwait(false);
        try
        {
            await probeTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }

        if (!serverProcess.HasExited)
        {
            serverProcess.Kill(entireProcessTree: true);
        }
    }
}

static async ValueTask RunTcpPublishLoopAsync(
    IMqttClient publisher,
    byte[] payload,
    MQTTnet.Protocol.MqttQualityOfServiceLevel qualityOfServiceLevel,
    int maxPendingMessages,
    int deliveryMultiplier,
    Func<long> getReceivedMessages,
    long messageCount,
    CancellationToken cancellationToken)
{
    for (long published = 0; published < messageCount; published++)
    {
        while (published * deliveryMultiplier -
            getReceivedMessages() >
            maxPendingMessages)
        {
            await Task.Delay(
                TimeSpan.FromMilliseconds(1),
                cancellationToken).ConfigureAwait(false);
        }

        MqttApplicationMessage message =
            new MqttApplicationMessageBuilder()
                .WithTopic("bench/memory")
                .WithPayload(payload)
                .WithQualityOfServiceLevel(qualityOfServiceLevel)
                .Build();
        await publisher.PublishAsync(
            message,
            cancellationToken).ConfigureAwait(false);
    }
}

static async Task ReadProbeLinesAsync(
    StreamReader reader,
    BenchmarkMemoryProbeCollector collector,
    CancellationToken cancellationToken)
{
    const string Prefix = "MEMORYPROBE ";
    while (!cancellationToken.IsCancellationRequested)
    {
        string? line = await reader.ReadLineAsync(cancellationToken)
            .ConfigureAwait(false);
        if (line == null)
        {
            return;
        }

        if (!line.StartsWith(Prefix, StringComparison.Ordinal))
        {
            continue;
        }

        try
        {
            BrokerMemoryProbeSnapshot? probe =
                JsonSerializer.Deserialize(
                    line.AsSpan(Prefix.Length),
                    BrokerMemoryProbeJsonContext.Default
                        .BrokerMemoryProbeSnapshot);
            if (probe != null)
            {
                collector.Observe(probe);
            }
        }
        catch (JsonException)
        {
        }
    }
}

static async Task<BrokerMemoryProbeSnapshot> WaitForFreshProbeAsync(
    Process serverProcess,
    BenchmarkMemoryProbeCollector collector,
    int probeMilliseconds,
    CancellationToken cancellationToken)
{
    long requested = Stopwatch.GetTimestamp();
    try
    {
        await serverProcess.StandardInput
            .WriteLineAsync("PROBE-GC")
            .ConfigureAwait(false);
        await serverProcess.StandardInput
            .FlushAsync(cancellationToken)
            .ConfigureAwait(false);
    }
    catch (IOException)
    {
    }

    using CancellationTokenSource timeout =
        CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken);
    timeout.CancelAfter(TimeSpan.FromSeconds(5));
    while (true)
    {
        BrokerMemoryProbeSnapshot? latest = collector.Latest;
        if (latest != null && latest.TimestampTicks > requested)
        {
            return latest;
        }

        await Task.Delay(
            Math.Max(5, probeMilliseconds / 4),
            timeout.Token).ConfigureAwait(false);
    }
}

static async Task RunMemoryProbeAsync(
    MqttZeroBroker broker,
    BrokerMemoryProbeState state,
    int intervalMilliseconds,
    CancellationToken cancellationToken)
{
    const string Prefix = "MEMORYPROBE ";
    while (!cancellationToken.IsCancellationRequested)
    {
        if (state.TakeGarbageCollectionRequest())
        {
            GC.Collect(
                GC.MaxGeneration,
                GCCollectionMode.Forced,
                blocking: true,
                compacting: true);
            GC.WaitForPendingFinalizers();
            GC.Collect(
                GC.MaxGeneration,
                GCCollectionMode.Forced,
                blocking: true,
                compacting: true);
        }

        MqttZeroBrokerMemorySnapshot snapshot =
            broker.GetMemorySnapshotForTest();
        using Process process = Process.GetCurrentProcess();
        BrokerMemoryProbeSnapshot probe = new(
            Stopwatch.GetTimestamp(),
            snapshot.BufferRentedBytes,
            snapshot.BufferCachedBytes,
            snapshot.BufferCommittedBytes,
            snapshot.RoutePlanAvailableCount,
            snapshot.RoutePlanInUseCount,
            snapshot.RoutePlanRetainedBytes,
            snapshot.SharedCursorCount,
            snapshot.PendingDeadlineCount,
            snapshot.ObjectPools.LeaseCount,
            snapshot.ObjectPools.QueuedMessageCount,
            snapshot.ObjectPools.PublishContextCount,
            snapshot.ObjectPools.InboundWorkCount,
            snapshot.ObjectPools.InboundShardWorkCount,
            snapshot.ObjectPools.FanoutOperationCount,
            snapshot.ObjectPools.FanoutBatchWorkCount,
            snapshot.ObjectPools.FanoutScheduledBatchCount,
            GC.GetTotalMemory(forceFullCollection: false),
            process.WorkingSet64,
            GC.GetTotalAllocatedBytes(precise: false),
            GC.CollectionCount(2));
        Console.Error.WriteLine(
            Prefix + JsonSerializer.Serialize(
                probe,
                BrokerMemoryProbeJsonContext.Default
                    .BrokerMemoryProbeSnapshot));
        try
        {
            await Task.Delay(
                intervalMilliseconds,
                cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }
    }
}

static async Task ReadProbeCommandsAsync(
    BrokerMemoryProbeState state,
    CancellationToken cancellationToken)
{
    while (!cancellationToken.IsCancellationRequested)
    {
        string? line;
        try
        {
            line = await Console.In.ReadLineAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        if (line == null)
        {
            return;
        }

        if (line.Trim() == "PROBE-GC")
        {
            state.RequestGarbageCollection();
        }
    }
}

static async ValueTask RunFixedPublishLoopAsync(
    MqttZeroBroker broker,
    MqttZeroPublishMessage message,
    int maxPendingMessages,
    int deliveryMultiplier,
    Func<long> getReceivedMessages,
    long messageCount,
    CancellationToken cancellationToken)
{
    for (long published = 0; published < messageCount; published++)
    {
        while (published * deliveryMultiplier -
            getReceivedMessages() >
            maxPendingMessages)
        {
            await Task.Delay(
                TimeSpan.FromMilliseconds(1),
                cancellationToken);
        }

        await broker.PublishAsync(message, cancellationToken);
    }
}

static void ForceGarbageCollection()
{
    GC.Collect(
        GC.MaxGeneration,
        GCCollectionMode.Forced,
        blocking: true,
        compacting: true);
    GC.WaitForPendingFinalizers();
    GC.Collect(
        GC.MaxGeneration,
        GCCollectionMode.Forced,
        blocking: true,
        compacting: true);
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
    BenchmarkEgressOptions egressOptions,
    long maxBufferedBytes = 0,
    int memoryProbeMilliseconds = 0)
{
    ProcessStartInfo startInfo = new()
    {
        FileName = "dotnet",
        RedirectStandardInput = true,
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
    if (maxBufferedBytes > 0)
    {
        startInfo.ArgumentList.Add("--max-buffered-bytes");
        startInfo.ArgumentList.Add(
            maxBufferedBytes.ToString(CultureInfo.InvariantCulture));
    }

    if (memoryProbeMilliseconds > 0)
    {
        startInfo.ArgumentList.Add("--memory-probe-ms");
        startInfo.ArgumentList.Add(
            memoryProbeMilliseconds.ToString(CultureInfo.InvariantCulture));
    }

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
    string? line;
    try
    {
        line = await process.StandardOutput.ReadLineAsync(
            timeout.Token);
    }
    catch (OperationCanceledException)
        when (!process.HasExited)
    {
        process.Kill(entireProcessTree: true);
        await process.WaitForExitAsync().ConfigureAwait(false);
        string error = await process.StandardError
            .ReadToEndAsync()
            .ConfigureAwait(false);
        throw new TimeoutException(
            $"Broker process did not become ready. Error: {error}");
    }

    if (line == "READY")
    {
        return;
    }

    string startupError = await process.StandardError
        .ReadToEndAsync()
        .ConfigureAwait(false);
    throw new InvalidOperationException(
        $"Broker process did not become ready. " +
        $"Output: {line}. Error: {startupError}");
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

static double ToAverageMicroseconds(long totalTicks, long sampleCount)
{
    return sampleCount == 0
        ? 0
        : totalTicks * 1_000_000d /
            Stopwatch.Frequency /
            sampleCount;
}

static void WriteProgress(
    int currentRun,
    int totalRuns,
    string broker,
    int qos,
    int subscribers,
    int payloadSize,
    BenchmarkRunProgress progress,
    BrokerBenchmarkResult? result = null)
{
    string details = result == null
        ? $"published={progress.PublishedMessages}, " +
            $"received={progress.ReceivedMessages}, " +
            $"failed={progress.FailedOperations}"
        : $"publish-window={result.PublishWindowThroughputPerSecond:F2} msg/s, " +
            $"end-to-end={result.EndToEndThroughputPerSecond:F2} msg/s, " +
            $"publish-p99={result.PublishCallLatencyP99Microseconds:F1} us, " +
            $"received={result.MeasurementReceivedMessages}/" +
            $"{result.PublishedMessages * subscribers}, " +
            $"drain={result.MeasurementDrainMilliseconds:F1} ms, " +
            $"failed={result.FailedOperations}";
    FormattableString line = $"[{currentRun}/{totalRuns}] {progress.Stage} broker={broker} qos={qos} subscribers={subscribers} payload={payloadSize} {details}";
    Console.Error.WriteLine(
        FormattableString.Invariant(line));
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

static long GetLong(
    IReadOnlyDictionary<string, string> arguments,
    string name,
    long defaultValue)
{
    return arguments.TryGetValue(name, out string? value)
        ? long.Parse(value, CultureInfo.InvariantCulture)
        : defaultValue;
}

static TimeSpan GetDrainTimeout(
    IReadOnlyDictionary<string, string> arguments)
{
    return TimeSpan.FromSeconds(
        GetInt(arguments, "drain-timeout", 30));
}

static TimeSpan GetReceiveDelay(BenchmarkRunOptions options)
{
    return TimeSpan.FromMicroseconds(
        options.SubscriberDelayMicroseconds);
}

static void EnsureClientSupportsExpiry(
    BenchmarkClientKind clientKind,
    BenchmarkRunOptions options)
{
    if (clientKind == BenchmarkClientKind.Raw &&
        options.MessageExpirySeconds > 0)
    {
        throw new ArgumentException(
            "The raw benchmark client does not support message expiry.");
    }
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
