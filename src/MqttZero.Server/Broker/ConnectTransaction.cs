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
/// 一次 CONNECT 处理的事务状态，失败时用于回滚。
/// </summary>
internal sealed class ConnectTransaction
{
    private MqttZeroSessionState? _session;

    /// <summary>
    /// 初始化 ConnectTransaction 的新实例。
    /// </summary>
    /// <param name="broker">对应的 broker。</param>
    /// <param name="connectPacket">连接报文。</param>
    /// <param name="output">对应的 output。</param>
    /// <param name="remoteEndPoint">远端EndPoint。</param>
    /// <param name="isSecure">对应的 isSecure。</param>
    /// <param name="isWebSocket">对应的 isWebSocket。</param>
    /// <param name="clientCertificate">客户端Certificate。</param>
    /// <param name="allowPacketFragmentation">allow报文Fragmentation。</param>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="assignedClientIdentifier">assigned客户端标识符。</param>
    /// <param name="sessionShardId">会话分片Id。</param>
    public ConnectTransaction(
        MqttZeroBroker broker,
        MqttConnectPacket connectPacket,
        PipeWriter output,
        EndPoint? remoteEndPoint,
        bool isSecure,
        bool isWebSocket,
        X509Certificate2? clientCertificate,
        bool allowPacketFragmentation,
        string clientId,
        bool assignedClientIdentifier,
        int sessionShardId)
    {
        ConnectPacket = connectPacket;
        ClientId = clientId;
        AssignedClientIdentifier = assignedClientIdentifier;
        EffectiveExpiry = broker.ClampSessionExpiry(
            connectPacket.SessionExpiryInterval ?? 0);
        Now = broker.BrokerTimeProvider.GetUtcNow();
        NewConnection = new MqttZeroClientConnection(
            output,
            remoteEndPoint,
            isSecure,
            isWebSocket,
            connectPacket.KeepAliveSeconds,
            connectPacket.ReceiveMaximum,
            connectPacket.MaximumPacketSize,
            broker.BrokerTimeProvider,
            broker.BufferPool,
            allowPacketFragmentation,
            connectPacket.RequestProblemInformation,
            connectPacket.RequestResponseInformation,
            sessionShardId);
        ClientInfo = new MqttZeroClientConnectionInfo
        {
            ClientId = clientId,
            UserName = connectPacket.HasUserName
                ? Encoding.UTF8.GetString(connectPacket.UserName.ToArray())
                : null,
            RemoteEndPoint = remoteEndPoint,
            IsSecure = isSecure,
            IsWebSocket = isWebSocket,
            ProtocolVersion = connectPacket.ProtocolVersion,
            ClientCertificate = clientCertificate
        };
        NewConnection.ClientInfo = ClientInfo;
    }

    /// <summary>
    /// 连接报文。
    /// </summary>
    public MqttConnectPacket ConnectPacket { get; }

    /// <summary>
    /// 客户端Id。
    /// </summary>
    public string ClientId { get; }

    /// <summary>
    /// Assigned客户端标识符。
    /// </summary>
    public bool AssignedClientIdentifier { get; }

    /// <summary>
    /// EffectiveExpiry 的值。
    /// </summary>
    public uint EffectiveExpiry { get; }

    /// <summary>
    /// 当前时间。
    /// </summary>
    public DateTimeOffset Now { get; }

    /// <summary>
    /// New连接。
    /// </summary>
    public MqttZeroClientConnection NewConnection { get; }

    /// <summary>
    /// 客户端Info。
    /// </summary>
    public MqttZeroClientConnectionInfo ClientInfo { get; }

    /// <summary>
    /// 获取或设置本次事务创建的会话；提交前访问会抛出异常。
    /// </summary>
    public MqttZeroSessionState Session
    {
        get
        {
            return _session ?? throw new InvalidOperationException(
                "连接事务的会话尚未创建。");
        }
        set
        {
            _session = value;
        }
    }

    /// <summary>
    /// 会话Present。
    /// </summary>
    public bool SessionPresent { get; set; }

    /// <summary>
    /// Replacing会话。
    /// </summary>
    public bool ReplacingSession { get; set; }

    /// <summary>
    /// New会话Persisted。
    /// </summary>
    public bool NewSessionPersisted { get; set; }

    /// <summary>
    /// ExistingPersistenceDeleted 的值。
    /// </summary>
    public bool ExistingPersistenceDeleted { get; set; }

    /// <summary>
    /// Replaced会话。
    /// </summary>
    public MqttZeroSessionState? ReplacedSession { get; set; }

    /// <summary>
    /// 前一个CleanStart。
    /// </summary>
    public bool PreviousCleanStart { get; set; }

    /// <summary>
    /// 前一个Expiry间隔。
    /// </summary>
    public uint PreviousExpiryInterval { get; set; }

    /// <summary>
    /// 前一个DisconnectedAt。
    /// </summary>
    public DateTimeOffset? PreviousDisconnectedAt { get; set; }

    /// <summary>
    /// 前一个遗嘱消息。
    /// </summary>
    public MqttZeroWillMessage? PreviousWillMessage { get; set; }

    /// <summary>
    /// 前一个遗嘱DueAt。
    /// </summary>
    public DateTimeOffset? PreviousWillDueAt { get; set; }

    /// <summary>
    /// 前一个Pump会话。
    /// </summary>
    public MqttZeroSessionState? PreviousPumpSession { get; set; }

    /// <summary>
    /// 前一个Pump。
    /// </summary>
    public MqttZeroSessionEgress? PreviousPump { get; set; }

    /// <summary>
    /// 前一个PumpStopped。
    /// </summary>
    public bool PreviousPumpStopped { get; set; }

    /// <summary>
    /// Old连接。
    /// </summary>
    public MqttZeroClientConnection? OldConnection { get; set; }

    /// <summary>
    /// Takeover遗嘱。
    /// </summary>
    public MqttZeroWillMessage? TakeoverWill { get; set; }
}
