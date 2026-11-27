namespace MqttZero.Server.Interceptors;

/// <summary>
/// CONNECT 处理的拦截结果。
/// </summary>
public enum MqttZeroConnectionInterceptionResult
{
    /// <summary>Continue processing.</summary>
    Continue,
    /// <summary>Reject with a CONNACK reason.</summary>
    Reject,
    /// <summary>Close without a CONNACK.</summary>
    Drop
}
