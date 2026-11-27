using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Scheduling;
using MqttZero.Server.Sessions;
using MqttZero.Server.Storage;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Text;
using System.Security.Cryptography.X509Certificates;
using MqttZero.Server.Observers;
using MqttZero.Server.Clients;
using MqttZero.Server.Egress;

namespace MqttZero.Server.Broker;

/// <summary>
/// CONNECT 完成后挂到会话上的连接信息。
/// </summary>
internal sealed class ConnectionAttachment
{
    /// <summary>
    /// 初始化 ConnectionAttachment 的新实例。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="connection">连接。</param>
    public ConnectionAttachment(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection)
    {
        Session = session;
        Connection = connection;
    }

    /// <summary>
    /// 会话。
    /// </summary>
    public MqttZeroSessionState Session { get; }

    /// <summary>
    /// 连接。
    /// </summary>
    public MqttZeroClientConnection Connection { get; }
}
