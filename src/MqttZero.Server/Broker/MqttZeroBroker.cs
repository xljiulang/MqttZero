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
/// MQTT 5 Broker 主体，按连接、会话、路由、投递与生命周期拆成多个分部文件。
/// </summary>
public sealed partial class MqttZeroBroker : IMqttZeroBroker, IAsyncDisposable
{
    // Lock hierarchy: _lifecycleGate -> _routeMutationGate, and
    // _retainedMutationGate -> _retainedGate. _sessionGate and _retainedGate
    // are leaves. Every write to one PipeWriter must run through the owning
    // connection's write gate or on its session shard.
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
    private readonly ConcurrentDictionary<string, MqttZeroRetainedMessage> _retainedMessages = new(StringComparer.Ordinal);
    private MqttZeroRetainedTopicTrie _retainedTopicTrie = MqttZeroRetainedTopicTrie.Empty;
    private readonly ConcurrentDictionary<string, MqttZeroSessionState> _sessions = new(StringComparer.Ordinal);
    private readonly MqttZeroSharedGroupCursors _sharedGroupCursors = new();
    private readonly Dictionary<MqttZeroSharedGroupKey, int> _sharedGroupReferences = [];
    private readonly ConcurrentDictionary<long, CancellationTokenSource> _acceptedConnections = new();
    private readonly SemaphoreSlim _lifecycleGate = new(1, 1);
    private readonly SemaphoreSlim _retainedMutationGate = new(1, 1);
    private readonly SemaphoreSlim _routeMutationGate = new(1, 1);
    private readonly object _retainedGate = new();
    private readonly MqttZeroFanoutDiagnostics _fanoutDiagnostics = new();
    private readonly MqttZeroRouteEpochManager _routeEpochs;
    private MqttZeroFanoutDispatcher? _fanoutDispatcher;
    private MqttZeroRoutePlanBufferPool? _qosZeroPlanBuffers;
    private MqttZeroRoutePlannerLanes? _routePlannerLanes;
    private MqttZeroSessionShardMap? _sessionShardMap;
    private MqttZeroShardSet? _shardSet;
    private readonly MqttZeroOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<MqttZeroBroker> _logger;
    private TaskCompletionSource _connectionsDrained = CreateCompletedDrainSignal();
    private long _nextAcceptedConnectionId;
    private int _activeConnectionCount;
    private int _connectedClientCount;
    private int _disposed;
    private long _publishedWillCount;
    private long _retainedBytes;
    private int _stopping;
    private int _state;
    private long _nextMessageId;

    /// <summary>
    /// 获取下一个消息Id。
    /// </summary>
    /// <returns>计算得到的数值。</returns>
    internal long GetNextMessageId()
    {
        return Interlocked.Increment(ref _nextMessageId);
    }

    /// <summary>
    /// 获取 Broker 配置。
    /// </summary>
    internal MqttZeroOptions BrokerOptions
    {
        get
        {
            return _options;
        }
    }

    /// <summary>
    /// 获取 Broker 使用的时间提供者。
    /// </summary>
    internal TimeProvider BrokerTimeProvider
    {
        get
        {
            return _timeProvider;
        }
    }

    /// <summary>
    /// 获取 Broker 使用的缓冲区池。
    /// </summary>
    internal MqttZeroBufferPool BufferPool
    {
        get
        {
            return _bufferPool;
        }
    }

    /// <summary>
    /// 分配下一个已受理连接标识。
    /// </summary>
    /// <returns>递增后的连接标识。</returns>
    internal long NextAcceptedConnectionId()
    {
        return Interlocked.Increment(ref _nextAcceptedConnectionId);
    }

    /// <summary>
    /// 创建会话状态。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <returns>对应的结果。</returns>
    internal MqttZeroSessionState CreateSessionState(string clientId)
    {
        return new MqttZeroSessionState(
            clientId,
            _options.MaxMessageQueueLength,
            SessionShardMap.GetShardId(clientId));
    }

    /// <summary>
    /// ShardSet 的值。
    /// </summary>
    private MqttZeroShardSet ShardSet
    {
        get
        {
            return Volatile.Read(ref _shardSet) ??
                throw new InvalidOperationException(
                    "The MQTTZero event loop shards are not running.");
        }
    }

    /// <summary>
    /// SessionShardMap 的值。
    /// </summary>
    private MqttZeroSessionShardMap SessionShardMap
    {
        get
        {
            return _sessionShardMap ??
                throw new InvalidOperationException(
                    "The session shard map is not initialized.");
        }
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
        _routeEpochs = new MqttZeroRouteEpochManager(_sharedGroupCursors);
    }

    /// <summary>
    /// 创建。
    /// </summary>
    /// <param name="options">选项。</param>
    /// <param name="timeProvider">时间Provider。</param>
    /// <param name="publishInterceptors">发布Interceptors。</param>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// ShardCount 的值。
    /// </summary>
    internal int ShardCount
    {
        get
        {
            return Volatile.Read(ref _shardSet)?.Count ?? 0;
        }
    }

    /// <summary>
    /// Shards 的值。
    /// </summary>
    internal MqttZeroShardSet? Shards
    {
        get
        {
            return Volatile.Read(ref _shardSet);
        }
    }

    /// <summary>
    /// RouteEpoch 的值。
    /// </summary>
    internal MqttZeroRouteEpoch RouteEpoch
    {
        get
        {
            return _routeEpochs.Current;
        }
    }

    /// <summary>
    /// RetainedTopicTrieForTest 的值。
    /// </summary>
    internal MqttZeroRetainedTopicTrie RetainedTopicTrieForTest
    {
        get
        {
            return Volatile.Read(ref _retainedTopicTrie);
        }
    }

    /// <summary>
    /// 获取Fanout诊断Test。
    /// </summary>
    internal MqttZeroFanoutDiagnosticsSnapshot
        GetFanoutDiagnosticsForTest()
    {
        return _fanoutDiagnostics.GetSnapshot();
    }

    /// <summary>
    /// 重置Fanout诊断Test。
    /// </summary>
    internal void ResetFanoutDiagnosticsForTest()
    {
        _fanoutDiagnostics.Reset();
    }

    /// <summary>
    /// 获取内存快照Test。
    /// </summary>
    /// <returns>对应的结果。</returns>
    internal MqttZeroBrokerMemorySnapshot GetMemorySnapshotForTest()
    {
        MqttZeroBufferPoolSnapshot buffers = _bufferPool.GetSnapshot();
        MqttZeroRoutePlanBufferPoolSnapshot routePlans =
            _qosZeroPlanBuffers?.GetSnapshot() ??
            new MqttZeroRoutePlanBufferPoolSnapshot(0, 0, 0);
        MqttZeroObjectPoolSnapshot objectPools = new(
            MqttZeroMessageLease.PoolCountForTest,
            MqttZeroQueuedMessage.PoolCountForTest,
            MqttZeroPublishContext.PoolCountForTest,
            MqttZeroInboundWork.PoolCountForTest,
            MqttZeroInboundShardWork.PoolCountForTest,
            MqttZeroFanoutOperation.PoolCountForTest,
            MqttZeroFanoutBatchWork.PoolCountForTest,
            MqttZeroFanoutScheduledBatch.PoolCountForTest,
            MqttZeroAcknowledgementWork.PoolCountForTest,
            MqttZeroAcknowledgementShardWork.PoolCountForTest,
            MqttZero.Server.Egress.MqttZeroControlPostWork.PoolCountForTest);
        return new MqttZeroBrokerMemorySnapshot(
            buffers.RentedBytes,
            buffers.CachedBytes,
            buffers.CommittedBytes,
            routePlans.AvailableCount,
            routePlans.InUseCount,
            routePlans.RetainedBytes,
            _sharedGroupCursors.Count,
            objectPools,
            Volatile.Read(ref _shardSet)?.PendingTimerCount ?? 0);
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

    /// <summary>
    /// 创建排空Signal。
    /// </summary>
    /// <returns>对应的结果。</returns>
    private static TaskCompletionSource CreateDrainSignal()
    {
        return new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    /// <summary>
    /// 创建Completed排空Signal。
    /// </summary>
    /// <returns>对应的结果。</returns>
    private static TaskCompletionSource CreateCompletedDrainSignal()
    {
        TaskCompletionSource signal = CreateDrainSignal();
        signal.SetResult();
        return signal;
    }
}
