using MqttZero.Protocol;

namespace MqttZero.Server.Observers;

/// <summary>
/// 观察连接生命周期事件。
/// </summary>
public interface IMqttZeroConnectionObserver
{
    /// <summary>Called after a client is connected.</summary>
    ValueTask OnConnectedAsync(
        MqttZeroClientConnectedContext context,
        CancellationToken cancellationToken);
    /// <summary>Called after a client connection ends.</summary>
    ValueTask OnDisconnectedAsync(
        MqttZeroClientDisconnectedContext context,
        CancellationToken cancellationToken);
}
