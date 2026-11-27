namespace MqttZero.Server.Clients;

/// <summary>
/// 连接写门的持有凭据。
/// </summary>
internal readonly struct MqttZeroWriteLease : IDisposable
{
    private readonly MqttZeroClientConnection? _connection;

    /// <summary>
    /// 初始化 MqttZeroWriteLease 的新实例。
    /// </summary>
    /// <param name="connection">连接。</param>
    internal MqttZeroWriteLease(MqttZeroClientConnection connection)
    {
        _connection = connection;
    }

    /// <summary>
    /// 释放。
    /// </summary>
    public void Dispose()
    {
        _connection?.ReleaseWrite();
    }
}
