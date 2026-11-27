namespace MqttZero.Server.Observers;

/// <summary>
/// 连接结束的原因分类。
/// </summary>
public enum MqttZeroDisconnectType
{
    /// <summary>The client sent DISCONNECT.</summary>
    ClientRequest,
    /// <summary>The transport was lost.</summary>
    ConnectionLost,
    /// <summary>The keep-alive expired.</summary>
    KeepAliveTimeout,
    /// <summary>Another connection took over the session.</summary>
    SessionTakenOver,
    /// <summary>The broker stopped.</summary>
    ServerShutdown,
    /// <summary>The protocol or broker pipeline failed.</summary>
    ProtocolError
}
