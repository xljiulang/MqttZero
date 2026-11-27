namespace MqttZero.Server;

/// <summary>
/// Broker 行为与资源上限的配置。
/// </summary>
public sealed class MqttZeroOptions
{
    /// <summary>
    /// 获取或设置同时受理的活跃连接上限。
    /// </summary>
    public int MaxActiveConnections { get; set; } = 20_000;

    /// <summary>
    /// 获取或设置保存的会话数量上限。
    /// </summary>
    public int MaxSessions { get; set; } = 20_000;

    /// <summary>
    /// 获取或设置事件循环分片数量。取 0 时按逻辑处理器数量解析。
    /// </summary>
    public int EventLoopShardCount { get; set; }

    /// <summary>
    /// 获取或设置单个会话允许的最大订阅数。
    /// </summary>
    public int MaxSubscriptionsPerSession { get; set; } = 1_000;

    /// <summary>
    /// 获取或设置单个会话未确认的出站 QoS 1/2 消息上限。
    /// </summary>
    public int MaxInflight { get; set; } = 32;

    /// <summary>
    /// 获取或设置单次出站数据批次的编码字节上限。
    /// </summary>
    public int OutboundBatchBytes { get; set; } = 512 * 1024;

    /// <summary>
    /// 获取或设置单次出站数据批次的消息条数上限。
    /// </summary>
    public int OutboundBatchMessages { get; set; } = 128;

    /// <summary>
    /// 获取或设置单次批次的选批与编码时间上限；不含网络 flush 时间。
    /// </summary>
    public TimeSpan OutboundBatchDuration { get; set; } = TimeSpan.FromMicroseconds(500);

    /// <summary>
    /// 获取或设置入站 QoS 2 中等待 PUBREL 的消息上限。
    /// </summary>
    public int MaxAwaitingRel { get; set; } = 100;

    /// <summary>
    /// 获取或设置单个会话的排队消息上限。
    /// </summary>
    public int MaxMessageQueueLength { get; set; } = 1_000;

    /// <summary>
    /// 获取或设置拥塞或离线时是否把 QoS 0 消息入队。
    /// </summary>
    public bool StoreQos0InQueue { get; set; } = true;

    /// <summary>
    /// 获取或设置可接受的 MQTT 报文最大字节数。
    /// </summary>
    public int MaxPacketSize { get; set; } = 1024 * 1024;

    /// <summary>
    /// 获取或设置 Broker 可用的池化字节上限。
    /// </summary>
    public long MaxBufferedBytes { get; set; } = 512L * 1024 * 1024;

    /// <summary>
    /// 获取或设置保留消息数量上限；取 0 表示不限数量。
    /// </summary>
    public int MaxRetainedMessages { get; set; }

    /// <summary>
    /// 获取或设置保留消息的负载字节总上限。
    /// </summary>
    public long MaxRetainedBytes { get; set; } = 256L * 1024 * 1024;

    /// <summary>
    /// 获取或设置等待 MQTT CONNECT 报文的超时时间。
    /// </summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// 获取或设置等待不完整 MQTT 报文体的超时时间。
    /// </summary>
    public TimeSpan IncompletePacketTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// 获取或设置接受客户端声明的最大会话过期时间；为空表示 Broker 侧不做上限。
    /// </summary>
    public TimeSpan? MaxSessionExpiryInterval { get; set; }

    /// <summary>
    /// 获取或设置保活与会话维护的检查间隔。
    /// </summary>
    public TimeSpan KeepAliveCheckInterval { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// 获取或设置正在读取负载的客户端是否允许超过保活期限。
    /// </summary>
    public bool DisconnectClientWhenReadingPayload { get; set; }

    /// <summary>
    /// 获取或设置向客户端宣告的主题别名上限。
    /// </summary>
    public ushort TopicAliasMaximum { get; set; } = ushort.MaxValue;

    /// <summary>
    /// 获取或设置客户端请求时返回的可选响应信息。
    /// </summary>
    public string? ResponseInformation { get; set; }

    /// <summary>
    /// 获取或设置是否受理新连接。
    /// </summary>
    public bool AcceptNewConnections { get; set; } = true;
}
