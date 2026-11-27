namespace MqttZero.Server.Interceptors;

/// <summary>
/// 单个接收者投递的拦截结果。
/// </summary>
public enum MqttZeroEnqueueInterceptionResult
{
    /// <summary>Enqueue or send the delivery.</summary>
    Enqueue,
    /// <summary>Drop this receiver delivery.</summary>
    Drop,
    /// <summary>Close the sender connection.</summary>
    CloseSenderConnection
}
