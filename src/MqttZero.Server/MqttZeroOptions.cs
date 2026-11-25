namespace MqttZero.Server;

/// <summary>
/// Configures MQTTZero broker behavior and resource limits.
/// </summary>
public sealed class MqttZeroOptions
{
    /// <summary>
    /// Gets or sets the maximum number of active accepted connections.
    /// </summary>
    public int MaxActiveConnections { get; set; } = 20_000;

    /// <summary>
    /// Gets or sets the maximum number of stored sessions.
    /// </summary>
    public int MaxSessions { get; set; } = 20_000;

    /// <summary>
    /// Gets or sets the number of event loop shards. Zero resolves to the
    /// logical processor count.
    /// </summary>
    public int EventLoopShardCount { get; set; }

    /// <summary>
    /// Gets or sets the maximum subscriptions allowed for one session.
    /// </summary>
    public int MaxSubscriptionsPerSession { get; set; } = 1_000;

    /// <summary>
    /// Gets or sets the maximum number of unacknowledged outbound QoS 1/2 messages.
    /// </summary>
    public int MaxInflight { get; set; } = 32;

    /// <summary>
    /// Gets or sets the maximum encoded bytes selected for one outbound data batch.
    /// </summary>
    public int OutboundBatchBytes { get; set; } = 512 * 1024;

    /// <summary>
    /// Gets or sets the maximum number of messages selected for one outbound data batch.
    /// </summary>
    public int OutboundBatchMessages { get; set; } = 128;

    /// <summary>
    /// Gets or sets the maximum data selection and encoding duration for one batch.
    /// Network flush time is not included.
    /// </summary>
    public TimeSpan OutboundBatchDuration { get; set; } =
        TimeSpan.FromMicroseconds(500);

    /// <summary>
    /// Gets or sets the maximum number of inbound QoS 2 messages awaiting PUBREL.
    /// </summary>
    public int MaxAwaitingRel { get; set; } = 100;

    /// <summary>
    /// Gets or sets the maximum number of queued messages for one session.
    /// </summary>
    public int MaxMessageQueueLength { get; set; } = 1_000;

    /// <summary>
    /// Gets or sets a value indicating whether QoS 0 messages are queued while congested or offline.
    /// </summary>
    public bool StoreQos0InQueue { get; set; } = true;

    /// <summary>
    /// Gets or sets the maximum accepted MQTT packet size.
    /// </summary>
    public int MaxPacketSize { get; set; } = 1024 * 1024;

    /// <summary>
    /// Gets or sets the maximum pooled bytes available to the broker.
    /// </summary>
    public long MaxBufferedBytes { get; set; } = 512L * 1024 * 1024;

    /// <summary>
    /// Gets or sets the maximum number of retained messages. Zero means no count limit.
    /// </summary>
    public int MaxRetainedMessages { get; set; }

    /// <summary>
    /// Gets or sets the maximum total retained payload bytes.
    /// </summary>
    public long MaxRetainedBytes { get; set; } = 256L * 1024 * 1024;

    /// <summary>
    /// Gets or sets the time allowed for the MQTT CONNECT packet.
    /// </summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Gets or sets the time allowed for an incomplete MQTT packet body.
    /// </summary>
    public TimeSpan IncompletePacketTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the maximum session expiry interval accepted from clients.
    /// A null value applies no broker-side cap.
    /// </summary>
    public TimeSpan? MaxSessionExpiryInterval { get; set; }

    /// <summary>
    /// Gets or sets the interval used for keep-alive and session maintenance.
    /// </summary>
    public TimeSpan KeepAliveCheckInterval { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// Gets or sets a value indicating whether clients reading a payload may exceed keep-alive.
    /// </summary>
    public bool DisconnectClientWhenReadingPayload { get; set; }

    /// <summary>
    /// Gets or sets the maximum topic alias advertised to clients.
    /// </summary>
    public ushort TopicAliasMaximum { get; set; } = ushort.MaxValue;

    /// <summary>
    /// Gets or sets optional response information returned when requested by a client.
    /// </summary>
    public string? ResponseInformation { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether new connections are accepted.
    /// </summary>
    public bool AcceptNewConnections { get; set; } = true;
}
