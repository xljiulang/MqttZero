namespace MqttZero.Server.Interceptors;

/// <summary>
/// CONNECT 处理的拦截结果。
/// </summary>
public enum MqttZeroConnectionInterceptionResult
{
    /// <summary>继续处理。</summary>
    Continue,
    /// <summary>用 CONNACK 原因码拒绝。</summary>
    Reject,
    /// <summary>不发送 CONNACK 直接关闭。</summary>
    Drop
}
