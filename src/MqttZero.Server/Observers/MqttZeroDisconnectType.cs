namespace MqttZero.Server.Observers;

/// <summary>
/// 连接结束的原因分类。
/// </summary>
public enum MqttZeroDisconnectType
{
    /// <summary>客户端发送了 DISCONNECT。</summary>
    ClientRequest,
    /// <summary>传输层断开。</summary>
    ConnectionLost,
    /// <summary>保活超时。</summary>
    KeepAliveTimeout,
    /// <summary>另一条连接接管了该会话。</summary>
    SessionTakenOver,
    /// <summary>Broker 已停止运行。</summary>
    ServerShutdown,
    /// <summary>协议或 Broker 管线发生故障。</summary>
    ProtocolError
}
