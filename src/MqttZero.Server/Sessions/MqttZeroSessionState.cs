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

    public List<MqttZeroSubscription> Subscriptions { get; } = [];

    public ConcurrentDictionary<object, object?> Items { get; } = new();

    public MqttZeroMessageQueue PendingMessages { get; }

    public Dictionary<ushort, MqttZeroQueuedMessage> InflightMessages { get; } = [];

    public Dictionary<ushort, MqttZeroMessageLease> InboundQos2Messages { get; } = [];

    public Dictionary<ushort, MqttReasonCode> RoutedInboundQos2Messages { get; } = [];

    public MqttZeroClientConnection? Connection { get; set; }

    public MqttZeroSessionEgress? Egress { get; set; }

    public bool CleanStart { get; set; }

    public uint ExpiryInterval { get; set; }

    public DateTimeOffset? DisconnectedAt { get; set; }

    public MqttZeroWillMessage? WillMessage { get; set; }

    public DateTimeOffset? WillDueAt { get; set; }

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
        int subscriptionCount;
        int pendingCount;
        int inflightCount;
        int inboundQos2Count;
        bool isConnected;
        bool cleanStart;
        uint expiryInterval;
        DateTimeOffset? expiresAt;
        MqttZeroWillMessage? willMessage;
        lock (SyncRoot)
        {
            subscriptionCount = Subscriptions.Count;
            pendingCount = PendingMessages.Count;
            inflightCount = InflightMessages.Count;
            inboundQos2Count = InboundQos2Messages.Count;
            isConnected = Connection != null;
            cleanStart = CleanStart;
            expiryInterval = ExpiryInterval;
            expiresAt = DisconnectedAt.HasValue && ExpiryInterval > 0
                ? DisconnectedAt.Value.AddSeconds(ExpiryInterval)
                : null;
            willMessage = WillMessage;
        }

        return new MqttZeroSessionSnapshot
        {
            ClientId = ClientId,
            IsConnected = isConnected,
            CleanStart = cleanStart,
            SessionPresent = true,
            ExpiryIntervalSeconds = expiryInterval,
            ExpiresAt = expiresAt,
            SubscriptionCount = subscriptionCount,
            PendingMessageCount = pendingCount,
            InflightMessageCount = inflightCount,
            InboundQos2MessageCount = inboundQos2Count,
            HasWill = willMessage != null,
            WillTopic = willMessage == null ? null : Encoding.UTF8.GetString(willMessage.Topic),
            SampledAt = now
        };
    }
}
