using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Clients;
using MqttZero.Server.Egress;
using MqttZero.Server.Scheduling;
using System.Collections.Concurrent;
using System.Text;

namespace MqttZero.Server.Sessions;

/// <summary>
/// 一个 MQTT 会话的可变状态。
/// </summary>
internal sealed class MqttZeroSessionState
{
    private MqttZeroTimerNode? _keepAliveTimer;
    private MqttZeroTimerNode? _messageExpiryTimer;
    private int _nextPacketIdentifier;
    private MqttZeroTimerNode? _sessionExpiryTimer;
    private MqttZeroTimerNode? _willTimer;
    private MqttZeroSessionLifecycleSnapshot _lifecycle = new(null, false, 0, null, null, null);
    private MqttZeroSessionEgress? _egress;
    private MqttZeroClientConnectionInfo? _offlineClientInfo;

    /// <summary>
    /// 初始化 MqttZeroSessionState 的新实例。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    public MqttZeroSessionState(string clientId)
        : this(clientId, 1000, 0)
    {
    }

    /// <summary>
    /// 初始化 MqttZeroSessionState 的新实例。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="maxQueueLength">max队列长度。</param>
    public MqttZeroSessionState(string clientId, int maxQueueLength)
        : this(clientId, maxQueueLength, 0)
    {
    }

    /// <summary>
    /// 初始化 MqttZeroSessionState 的新实例。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="maxQueueLength">max队列长度。</param>
    /// <param name="shardId">分片Id。</param>
    public MqttZeroSessionState(
        string clientId,
        int maxQueueLength,
        int shardId)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(shardId);
        ClientId = clientId;
        ShardId = shardId;
        PendingMessages = new MqttZeroMessageQueue(maxQueueLength);
    }

    /// <summary>
    /// 客户端Id。
    /// </summary>
    public string ClientId { get; }

    /// <summary>
    /// 获取表示该会话离线身份的客户端信息，首次访问时创建并缓存。
    /// </summary>
    internal MqttZeroClientConnectionInfo OfflineClientInfo
    {
        get
        {
            return _offlineClientInfo ??= new MqttZeroClientConnectionInfo
            {
                ClientId = ClientId
            };
        }
    }

    /// <summary>
    /// 分片Id。
    /// </summary>
    public int ShardId { get; }

    /// <summary>
    /// 订阅。
    /// </summary>
    public MqttZeroSubscriptionCollection Subscriptions { get; } = new();

    /// <summary>
    /// 项。
    /// </summary>
    public ConcurrentDictionary<object, object?> Items { get; } = new();

    /// <summary>
    /// 待处理消息。
    /// </summary>
    public MqttZeroMessageQueue PendingMessages { get; }

    /// <summary>
    /// 在途消息。
    /// </summary>
    public MqttZeroSessionMessageMap<MqttZeroQueuedMessage> InflightMessages { get; } = new();

    /// <summary>
    /// 入站Qos2消息。
    /// </summary>
    public MqttZeroSessionMessageMap<MqttZeroMessageLease> InboundQos2Messages { get; } = new();

    /// <summary>
    /// Routed入站Qos2消息。
    /// </summary>
    public MqttZeroSessionMessageMap<MqttReasonCode> RoutedInboundQos2Messages { get; } = new();

    /// <summary>
    /// Connection 的值。
    /// </summary>
    public MqttZeroClientConnection? Connection
    {
        get
        {
            return Lifecycle.Connection;
        }
        set
        {
            UpdateLifecycle(
                static (current, connection) =>
                    current with
                    {
                        Connection = connection
                    },
                value);
        }
    }

    /// <summary>
    /// Egress 的值。
    /// </summary>
    public MqttZeroSessionEgress? Egress
    {
        get
        {
            return Volatile.Read(ref _egress);
        }
        set
        {
            Volatile.Write(ref _egress, value);
        }
    }

    /// <summary>
    /// CleanStart 的值。
    /// </summary>
    public bool CleanStart
    {
        get
        {
            return Lifecycle.CleanStart;
        }
        set
        {
            UpdateLifecycle(
                static (current, cleanStart) =>
                    current with
                    {
                        CleanStart = cleanStart
                    },
                value);
        }
    }

    /// <summary>
    /// ExpiryInterval 的值。
    /// </summary>
    public uint ExpiryInterval
    {
        get
        {
            return Lifecycle.ExpiryInterval;
        }
        set
        {
            UpdateLifecycle(
                static (current, expiryInterval) =>
                    current with
                    {
                        ExpiryInterval = expiryInterval
                    },
                value);
        }
    }

    /// <summary>
    /// DisconnectedAt 的值。
    /// </summary>
    public DateTimeOffset? DisconnectedAt
    {
        get
        {
            return Lifecycle.DisconnectedAt;
        }
        set
        {
            UpdateLifecycle(
                static (current, disconnectedAt) =>
                    current with
                    {
                        DisconnectedAt = disconnectedAt
                    },
                value);
        }
    }

    /// <summary>
    /// WillMessage 的值。
    /// </summary>
    public MqttZeroWillMessage? WillMessage
    {
        get
        {
            return Lifecycle.WillMessage;
        }
        set
        {
            UpdateLifecycle(
                static (current, willMessage) =>
                    current with
                    {
                        WillMessage = willMessage
                    },
                value);
        }
    }

    /// <summary>
    /// WillDueAt 的值。
    /// </summary>
    public DateTimeOffset? WillDueAt
    {
        get
        {
            return Lifecycle.WillDueAt;
        }
        set
        {
            UpdateLifecycle(
                static (current, willDueAt) =>
                    current with
                    {
                        WillDueAt = willDueAt
                    },
                value);
        }
    }

    /// <summary>
    /// Lifecycle 的值。
    /// </summary>
    internal MqttZeroSessionLifecycleSnapshot Lifecycle
    {
        get
        {
            return Volatile.Read(ref _lifecycle);
        }
    }

    /// <summary>
    /// 取出出站。
    /// </summary>
    /// <returns>对应的结果。</returns>
    internal MqttZeroSessionEgress? TakeEgress()
    {
        return Interlocked.Exchange(ref _egress, null);
    }

    /// <summary>
    /// 尝试Attach出站。
    /// </summary>
    /// <param name="connection">连接。</param>
    /// <param name="egress">出站。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    internal bool TryAttachEgress(
        MqttZeroClientConnection connection,
        MqttZeroSessionEgress egress)
    {
        if (!ReferenceEquals(Connection, connection))
        {
            return false;
        }

        return Interlocked.CompareExchange(
            ref _egress,
            egress,
            null) == null;
    }

    /// <summary>
    /// 获取下一个报文标识符。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public ushort GetNextPacketIdentifier()
    {
        for (int attempt = 0; attempt < ushort.MaxValue; attempt++)
        {
            ushort candidate = (ushort)Interlocked.Increment(ref _nextPacketIdentifier);
            if (candidate != 0 && !InflightMessages.ContainsKey(candidate))
            {
                return candidate;
            }
        }

        throw new InvalidOperationException("No MQTT packet identifier is available.");
    }

    /// <summary>
    /// 获取定时器节点。
    /// </summary>
    /// <param name="kind">种类。</param>
    /// <returns>对应的结果。</returns>
    internal MqttZeroTimerNode GetTimerNode(MqttZeroDeadlineKind kind)
    {
        ref MqttZeroTimerNode? node = ref _keepAliveTimer;
        switch (kind)
        {
            case MqttZeroDeadlineKind.KeepAlive:
                break;
            case MqttZeroDeadlineKind.Will:
                node = ref _willTimer;
                break;
            case MqttZeroDeadlineKind.SessionExpiry:
                node = ref _sessionExpiryTimer;
                break;
            case MqttZeroDeadlineKind.MessageExpiry:
                node = ref _messageExpiryTimer;
                break;
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(kind),
                    kind,
                    "The timer kind is not owned by an MQTT session.");
        }

        MqttZeroTimerNode? current = Volatile.Read(ref node);
        if (current != null)
        {
            return current;
        }

        MqttZeroTimerNode candidate = new(kind, ClientId, this);
        return Interlocked.CompareExchange(
            ref node,
            candidate,
            null) ?? candidate;
    }

    /// <summary>
    /// 获取Existing定时器节点。
    /// </summary>
    /// <param name="kind">种类。</param>
    /// <returns>对应的结果。</returns>
    internal MqttZeroTimerNode? GetExistingTimerNode(
        MqttZeroDeadlineKind kind)
    {
        return kind switch
        {
            MqttZeroDeadlineKind.KeepAlive =>
                Volatile.Read(ref _keepAliveTimer),
            MqttZeroDeadlineKind.Will =>
                Volatile.Read(ref _willTimer),
            MqttZeroDeadlineKind.SessionExpiry =>
                Volatile.Read(ref _sessionExpiryTimer),
            MqttZeroDeadlineKind.MessageExpiry =>
                Volatile.Read(ref _messageExpiryTimer),
            _ => throw new ArgumentOutOfRangeException(
                nameof(kind),
                kind,
                "The timer kind is not owned by an MQTT session.")
        };
    }

    /// <summary>
    /// 创建快照。
    /// </summary>
    /// <param name="now">当前时间。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroSessionSnapshot CreateSnapshot(DateTimeOffset now)
    {
        MqttZeroSessionLifecycleSnapshot lifecycle = Lifecycle;
        DateTimeOffset? expiresAt =
            lifecycle.DisconnectedAt.HasValue &&
            lifecycle.ExpiryInterval > 0
                ? lifecycle.DisconnectedAt.Value.AddSeconds(
                    lifecycle.ExpiryInterval)
                : null;

        return new MqttZeroSessionSnapshot
        {
            ClientId = ClientId,
            IsConnected = lifecycle.IsConnected,
            CleanStart = lifecycle.CleanStart,
            SessionPresent = true,
            ExpiryIntervalSeconds = lifecycle.ExpiryInterval,
            ExpiresAt = expiresAt,
            SubscriptionCount = Subscriptions.Count,
            PendingMessageCount = PendingMessages.Count,
            InflightMessageCount = InflightMessages.Count,
            InboundQos2MessageCount = InboundQos2Messages.Count,
            HasWill = lifecycle.WillMessage != null,
            WillTopic = lifecycle.WillMessage == null
                ? null
                : Encoding.UTF8.GetString(
                    lifecycle.WillMessage.Topic),
            SampledAt = now
        };
    }

    /// <summary>
    /// 更新Lifecycle。
    /// </summary>
    /// <param name="update">对应的 update。</param>
    internal void UpdateLifecycle(
        Func<
            MqttZeroSessionLifecycleSnapshot,
            MqttZeroSessionLifecycleSnapshot> update)
    {
        ArgumentNullException.ThrowIfNull(update);
        while (true)
        {
            MqttZeroSessionLifecycleSnapshot current =
                Volatile.Read(ref _lifecycle);
            MqttZeroSessionLifecycleSnapshot replacement =
                update(current);
            if (ReferenceEquals(
                    Interlocked.CompareExchange(
                        ref _lifecycle,
                        replacement,
                        current),
                    current))
            {
                return;
            }
        }
    }

    /// <summary>
    /// 更新Lifecycle。
    /// </summary>
    /// <param name="update">对应的 update。</param>
    /// <param name="value">值。</param>
    private void UpdateLifecycle<T>(
        Func<MqttZeroSessionLifecycleSnapshot, T,
            MqttZeroSessionLifecycleSnapshot> update,
        T value)
    {
        while (true)
        {
            MqttZeroSessionLifecycleSnapshot current =
                Volatile.Read(ref _lifecycle);
            MqttZeroSessionLifecycleSnapshot replacement =
                update(current, value);
            if (ReferenceEquals(
                    Interlocked.CompareExchange(
                        ref _lifecycle,
                        replacement,
                        current),
                    current))
            {
                return;
            }
        }
    }
}
