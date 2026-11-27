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
    /// <summary>The broker has not been started.</summary>
    Created,

    /// <summary>The broker is starting.</summary>
    Starting,

    /// <summary>The broker is accepting connections.</summary>
    Running,

    /// <summary>The broker is stopping.</summary>
    Stopping,

    /// <summary>The broker has stopped.</summary>
    Stopped
}
