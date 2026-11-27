using MqttZero.Server.Clients;
using MqttZero.Protocol;
using MqttZero.Publishing;
using MqttZero.Server.Retained;
using MqttZero.Server.Sessions;

namespace MqttZero.Server.Broker;

/// <summary>
/// 面向使用者的 Broker 操作接口。
/// </summary>
public interface IMqttZeroBroker
{
    /// <summary>
    /// 获取 Broker 当前的运行状态。
    /// </summary>
    MqttZeroBrokerState State { get; }

    /// <summary>
    /// 获取已受理的活跃传输连接数。
    /// </summary>
    int ActiveConnectionCount { get; }

    /// <summary>
    /// 获取已连接的 MQTT 客户端数量。
    /// </summary>
    int ConnectedClientCount { get; }

    /// <summary>
    /// 获取 Broker 创建以来已发布的遗嘱消息数量。
    /// </summary>
    long PublishedWillCount { get; }

    /// <summary>
    /// 获取 Broker 可接受的最大 MQTT 报文大小。
    /// </summary>
    int MaximumPacketSize { get; }

    /// <summary>
    /// 发布一条服务端发起的消息给匹配的订阅者。
    /// </summary>
    /// <param name="message">待发布的消息。</param>
    /// <param name="cancellationToken">用于取消操作的令牌。</param>
    /// <returns>表示发布操作的异步任务。</returns>
    ValueTask PublishAsync(
        MqttZeroPublishMessage message,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 获取已连接客户端在某一时刻的快照。
    /// </summary>
    /// <returns>客户端快照集合。</returns>
    IReadOnlyList<MqttZeroClientSnapshot> GetClientSnapshots();

    /// <summary>
    /// 获取全部已保存会话的快照。
    /// </summary>
    /// <returns>会话快照集合。</returns>
    IReadOnlyList<MqttZeroSessionSnapshot> GetSessionSnapshots();

    /// <summary>
    /// 获取保留消息的元数据快照。
    /// </summary>
    /// <returns>保留消息快照集合。</returns>
    IReadOnlyList<MqttZeroRetainedMessageSnapshot> GetRetainedMessageSnapshots();

    /// <summary>
    /// 尝试按客户端标识获取已连接客户端的操作句柄。
    /// </summary>
    /// <param name="clientId">客户端标识。</param>
    /// <param name="client">找到时的已连接客户端句柄。</param>
    /// <returns><see langword="true"/> when the client is connected.</returns>
    bool TryGetClient(
        string clientId,
        out MqttZeroConnectedClient? client);

    /// <summary>
    /// 断开指定的已连接客户端。
    /// </summary>
    /// <param name="clientId">客户端标识。</param>
    /// <param name="reasonCode">DISCONNECT 报文使用的原因码。</param>
    /// <param name="cancellationToken">用于取消操作的令牌。</param>
    /// <returns><see langword="true"/> when a connected client was found.</returns>
    ValueTask<bool> DisconnectClientAsync(
        string clientId,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken = default);
}
