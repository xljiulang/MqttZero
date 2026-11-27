using MqttZero.Protocol;
namespace MqttZero.Server.Interceptors;

/// <summary>
/// 拦截器返回的通用控制结果。
/// </summary>
public enum MqttZeroInterceptionResult
{
    /// <summary>继续处理。</summary>
    Continue,

    /// <summary>消费当前操作，不再进入 Broker 流程。</summary>
    Consume,

    /// <summary>关闭当前客户端连接。</summary>
    CloseConnection
}
