using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
namespace MqttZero.Server.Broker;

/// <summary>
/// Broker 生命周期状态。
/// </summary>
public enum MqttZeroBrokerState
{
    /// <summary>Broker 尚未启动。</summary>
    Created,

    /// <summary>Broker 正在启动。</summary>
    Starting,

    /// <summary>Broker 正在受理连接。</summary>
    Running,

    /// <summary>Broker 正在停止。</summary>
    Stopping,

    /// <summary>Broker 已停止。</summary>
    Stopped
}
