using MqttZero.Protocol;

namespace MqttZero.Server.Observers;

/// <summary>
/// 观察连接生命周期事件。
/// </summary>
public interface IMqttZeroConnectionObserver
{
    /// <summary>客户端连接建立后调用。</summary>
    ValueTask OnConnectedAsync(
        MqttZeroClientConnectedContext context,
        CancellationToken cancellationToken);
    /// <summary>客户端连接结束后调用。</summary>
    ValueTask OnDisconnectedAsync(
        MqttZeroClientDisconnectedContext context,
        CancellationToken cancellationToken);
}
