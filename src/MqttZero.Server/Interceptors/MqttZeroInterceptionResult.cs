using MqttZero.Protocol;
namespace MqttZero.Server.Interceptors;

/// <summary>
/// 拦截器返回的通用控制结果。
/// </summary>
public enum MqttZeroInterceptionResult
{
    /// <summary>Continue processing.</summary>
    Continue,

    /// <summary>Consume the current operation without further broker processing.</summary>
    Consume,

    /// <summary>Close the current client connection.</summary>
    CloseConnection
}
