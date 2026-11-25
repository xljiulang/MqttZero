using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Server.Observers;
using MqttZero.Protocol;
using MqttZero.Server.Clients;
using MqttZero.Server.Retained;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using MqttZero.Server.Storage;
using MqttZero.Server.Scheduling;
using MqttZero.Server.Egress;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Text;

namespace MqttZero.Server.Broker;

/// <summary>
/// Provides the in-memory MQTT 5 broker pipeline.
/// </summary>
public sealed partial class MqttZeroBroker : IMqttZeroBroker, IAsyncDisposable
{
    private readonly object _sessionGate = new();
    private readonly MqttZeroBufferPool _bufferPool;
    private readonly IMqttZeroPublishInterceptor[] _publishInterceptors;
    private readonly IMqttZeroConnectionInterceptor[] _connectionInterceptors;
    private readonly IMqttZeroSubscribeInterceptor[] _subscribeInterceptors;
    private readonly IMqttZeroUnsubscribeInterceptor[] _unsubscribeInterceptors;
    private readonly IMqttZeroEnqueueInterceptor[] _enqueueInterceptors;
    private readonly IMqttZeroConnectionObserver[] _connectionObservers;
    private readonly IMqttZeroDeliveryObserver[] _deliveryObservers;
    private readonly IMqttZeroSessionStore _sessionStore;
    private readonly IMqttZeroMessageStore _messageStore;
    private readonly IMqttZeroRetainedMessageStore _retainedMessageStore;
    private readonly ConcurrentDictionary<string, MqttZeroRetainedMessage> _retainedMessages =
        new(StringComparer.Ordinal);
    private MqttZeroRetainedTopicTrie _retainedTopicTrie =
        MqttZeroRetainedTopicTrie.Empty;
    private readonly ConcurrentDictionary<string, MqttZeroSessionState> _sessions =
        new(StringComparer.Ordinal);
    private readonly MqttZeroSharedGroupCursors _sharedGroupCursors = new();
    private readonly List<(
        MqttZeroClientConnection Connection,
        MqttZeroEgressActor? Pump)> _keepAliveExpired = [];
    private readonly List<(MqttZeroSessionState Session, MqttZeroWillMessage Will)> _dueWills = [];
    private readonly List<MqttZeroSessionState> _expiredSessions = [];
    private readonly List<string> _expiredRetainedTopics = [];
    private readonly ConcurrentDictionary<long, CancellationTokenSource> _acceptedConnections = new();
    private readonly Dictionary<string, SessionMutationLock> _sessionMutationLocks = [];
    private readonly SemaphoreSlim _lifecycleGate = new(1, 1);
    private readonly SemaphoreSlim _retainedMutationGate = new(1, 1);
    private readonly SemaphoreSlim _routeMutationGate = new(1, 1);
    private readonly MqttZeroFanoutDiagnostics _fanoutDiagnostics = new();
    private readonly MqttZeroRouteEpochManager _routeEpochs = new();
    private MqttZeroFanoutDispatcher? _fanoutDispatcher;
    private MqttZeroRoutePlanBufferPool? _qosZeroPlanBuffers;
    private MqttZeroRoutePlannerLanes? _routePlannerLanes;
    private MqttZeroSessionShardMap? _sessionShardMap;
    private MqttZeroShardSet? _shardSet;
    private readonly MqttZeroOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<MqttZeroBroker> _logger;
    private TaskCompletionSource _connectionsDrained =
        CreateCompletedDrainSignal();
    private long _nextAcceptedConnectionId;
    private CancellationTokenSource? _maintenanceCancellation;
    private Task? _maintenanceTask;
    private int _activeConnectionCount;
    private int _connectedClientCount;
    private int _disposed;
    private long _publishedWillCount;
    private long _retainedBytes;
    private int _stopping;
    private int _fanoutFailed;
    private int _state;
    private long _nextMessageId;

    internal long GetNextMessageId()
    {
        return Interlocked.Increment(ref _nextMessageId);
    }

    internal MqttZeroSessionState CreateSessionState(string clientId)
    {
        MqttZeroSessionShardMap shardMap = _sessionShardMap ??
            throw new InvalidOperationException(
                "The session shard map is not initialized.");
        return new MqttZeroSessionState(
            clientId,
            _options.MaxMessageQueueLength,
            shardMap.GetShardId(clientId));
    }

    /// <summary>Initializes a broker and its extension pipeline.</summary>
    /// <param name="options">The broker options accessor.</param>
    /// <param name="timeProvider">The time provider.</param>
    /// <param name="logger">The broker logger.</param>
    /// <param name="publishInterceptors">The ordered publish interceptors.</param>
    /// <param name="connectionInterceptors">The ordered CONNECT interceptors.</param>
    /// <param name="subscribeInterceptors">The ordered subscribe interceptors.</param>
    /// <param name="unsubscribeInterceptors">The ordered unsubscribe interceptors.</param>
    /// <param name="enqueueInterceptors">The ordered enqueue interceptors.</param>
    /// <param name="connectionObservers">The ordered connection observers.</param>
    /// <param name="deliveryObservers">The ordered delivery observers.</param>
    /// <param name="sessionStore">The session persistence store.</param>
    /// <param name="messageStore">The message persistence store.</param>
    /// <param name="retainedMessageStore">The retained-message persistence store.</param>
    public MqttZeroBroker(
        IOptions<MqttZeroOptions> options,
        TimeProvider timeProvider,
        ILogger<MqttZeroBroker> logger,
        IEnumerable<IMqttZeroPublishInterceptor> publishInterceptors,
        IEnumerable<IMqttZeroConnectionInterceptor> connectionInterceptors,
        IEnumerable<IMqttZeroSubscribeInterceptor> subscribeInterceptors,
        IEnumerable<IMqttZeroUnsubscribeInterceptor> unsubscribeInterceptors,
        IEnumerable<IMqttZeroEnqueueInterceptor> enqueueInterceptors,
        IEnumerable<IMqttZeroConnectionObserver> connectionObservers,
        IEnumerable<IMqttZeroDeliveryObserver> deliveryObservers,
        IMqttZeroSessionStore sessionStore,
        IMqttZeroMessageStore messageStore,
        IMqttZeroRetainedMessageStore retainedMessageStore)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options.Value ?? throw new ArgumentNullException(nameof(options));
        MqttZeroOptionsValidator.Validate(_options);
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        ArgumentNullException.ThrowIfNull(publishInterceptors);
        _bufferPool = new MqttZeroBufferPool(_options.MaxBufferedBytes);
        _publishInterceptors = publishInterceptors.ToArray();
        _connectionInterceptors = connectionInterceptors.ToArray();
        _subscribeInterceptors = subscribeInterceptors.ToArray();
        _unsubscribeInterceptors = unsubscribeInterceptors.ToArray();
        _enqueueInterceptors = enqueueInterceptors.ToArray();
        _connectionObservers = connectionObservers.ToArray();
        _deliveryObservers = deliveryObservers.ToArray();
        _sessionStore = sessionStore ?? throw new ArgumentNullException(nameof(sessionStore));
        _messageStore = messageStore ?? throw new ArgumentNullException(nameof(messageStore));
        _retainedMessageStore = retainedMessageStore ??
            throw new ArgumentNullException(nameof(retainedMessageStore));
    }

    internal static MqttZeroBroker Create(
        MqttZeroOptions options,
        TimeProvider timeProvider,
        IEnumerable<IMqttZeroPublishInterceptor>? publishInterceptors = null)
    {
        return new MqttZeroBroker(
            Options.Create(options),
            timeProvider,
            NullLogger<MqttZeroBroker>.Instance,
            publishInterceptors ?? [],
            [],
            [],
            [],
            [],
            [],
            [],
            new MqttZeroNoopSessionStore(),
            new MqttZeroNoopMessageStore(),
            new MqttZeroNoopRetainedMessageStore());
    }

    /// <summary>
    /// Gets the current broker lifecycle state.
    /// </summary>
    public MqttZeroBrokerState State
    {
        get
        {
            return (MqttZeroBrokerState)Volatile.Read(ref _state);
        }
    }

    /// <summary>
    /// Gets the number of active accepted transport connections.
    /// </summary>
    public int ActiveConnectionCount
    {
        get
        {
            return Volatile.Read(ref _activeConnectionCount);
        }
    }

    /// <summary>
    /// Gets the number of connected MQTT clients.
    /// </summary>
    public int ConnectedClientCount
    {
        get
        {
            return Volatile.Read(ref _connectedClientCount);
        }
    }

    /// <summary>
    /// Gets the number of will messages published since broker creation.
    /// </summary>
    public long PublishedWillCount
    {
        get
        {
            return Volatile.Read(ref _publishedWillCount);
        }
    }

    internal int ShardCount
    {
        get
        {
            return Volatile.Read(ref _shardSet)?.Count ?? 0;
        }
    }

    internal MqttZeroShardSet? Shards
    {
        get
        {
            return Volatile.Read(ref _shardSet);
        }
    }

    internal MqttZeroRouteEpoch RouteEpoch
    {
        get
        {
            return _routeEpochs.Current;
        }
    }

    internal MqttZeroRetainedTopicTrie RetainedTopicTrieForTest
    {
        get
        {
            return Volatile.Read(ref _retainedTopicTrie);
        }
    }

    internal MqttZeroFanoutDiagnosticsSnapshot
        GetFanoutDiagnosticsForTest()
    {
        return _fanoutDiagnostics.GetSnapshot();
    }

    internal void ResetFanoutDiagnosticsForTest()
    {
        _fanoutDiagnostics.Reset();
    }

    /// <summary>
    /// Gets the time provider used by the broker.
    /// </summary>
    public TimeProvider TimeProvider
    {
        get
        {
            return _timeProvider;
        }
    }

    /// <summary>
    /// Stops the broker and releases pooled resources.
    /// </summary>
    /// <returns>A task representing disposal.</returns>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        try
        {
            await StopAsync().ConfigureAwait(false);
            _qosZeroPlanBuffers?.Dispose();
            _routePlannerLanes?.Dispose();
            _routeMutationGate.Dispose();
            _bufferPool.Dispose();
        }
        catch
        {
            Volatile.Write(ref _disposed, 0);
            throw;
        }
    }

    private static TaskCompletionSource CreateDrainSignal()
    {
        return new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private static TaskCompletionSource CreateCompletedDrainSignal()
    {
        TaskCompletionSource signal = CreateDrainSignal();
        signal.SetResult();
        return signal;
    }
}
