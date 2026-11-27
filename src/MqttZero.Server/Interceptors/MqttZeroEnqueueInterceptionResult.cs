namespace MqttZero.Server.Interceptors;

/// <summary>
/// 单个接收者投递的拦截结果。
/// </summary>
public enum MqttZeroEnqueueInterceptionResult
{
    /// <summary>入队或直接发送该投递。</summary>
    Enqueue,
    /// <summary>丢弃发给该接收者的投递。</summary>
    Drop,
    /// <summary>关闭发送方连接。</summary>
    CloseSenderConnection
}
