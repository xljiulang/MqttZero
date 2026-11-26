using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Clients;
using MqttZero.Server.Egress;
using MqttZero.Server.Scheduling;
using System.Collections.Concurrent;
using System.Text;

namespace MqttZero.Server.Sessions;

internal sealed class MqttZeroSessionState
{
    private MqttZeroTimerNode? _keepAliveTimer;
    private MqttZeroTimerNode? _messageExpiryTimer;
    private int _nextPacketIdentifier;
    private MqttZeroTimerNode? _sessionExpiryTimer;
    private MqttZeroTimerNode? _willTimer;
    private MqttZeroSessionLifecycleSnapshot _lifecycle = new(
        null,
        false,
        0,
        null,
        null,
        null);
    private MqttZeroSessionEgress? _egress;

    public MqttZeroSessionState(string clientId)
        : this(clientId, 1000, 0)
    {
    }

    public MqttZeroSessionState(string clientId, int maxQueueLength)
        : this(clientId, maxQueueLength, 0)
    {
    }

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

    public string ClientId { get; }

    public int ShardId { get; }

    public object SyncRoot { get; } = new();

    public MqttZeroSubscriptionCollection Subscriptions { get; } = new();

    public ConcurrentDictionary<object, object?> Items { get; } = new();

    public MqttZeroMessageQueue PendingMessages { get; }

    public MqttZeroSessionMessageMap<MqttZeroQueuedMessage>
        InflightMessages
    { get; } = new();

    public MqttZeroSessionMessageMap<MqttZeroMessageLease>
        InboundQos2Messages
    { get; } = new();

    public MqttZeroSessionMessageMap<MqttReasonCode>
        RoutedInboundQos2Messages
    { get; } = new();

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

    internal MqttZeroSessionLifecycleSnapshot Lifecycle
    {
        get
        {
            return Volatile.Read(ref _lifecycle);
        }
    }

    internal MqttZeroSessionEgress? TakeEgress()
    {
        return Interlocked.Exchange(ref _egress, null);
    }

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
