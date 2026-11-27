using MqttZero.Server.Clients;
using MqttZero.Protocol;

namespace MqttZero.Server.Observers;

/// <summary>
/// 客户端连接断开事件。
/// </summary>
public sealed class MqttZeroClientDisconnectedContext
{
    /// <summary>Gets the client information.</summary>
    public required MqttZeroClientConnectionInfo Client { get; init; }
    /// <summary>Gets the disconnect type.</summary>
    public MqttZeroDisconnectType DisconnectType { get; init; }
    /// <summary>Gets the MQTT reason code.</summary>
    public MqttReasonCode? ReasonCode { get; init; }
}
