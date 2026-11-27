using MqttZero.Server.Broker;
using MqttZero.Protocol;
using MqttZero.Publishing;
using MqttZero.Server.Sessions;

namespace MqttZero.Server.Clients;

/// <summary>
/// 已连接客户端的操作句柄。
/// </summary>
public sealed class MqttZeroConnectedClient
{
    private readonly MqttZeroBroker _broker;
    private readonly MqttZeroSessionState _session;
    private readonly MqttZeroClientConnection _connection;

    /// <summary>
    /// 初始化 MqttZeroConnectedClient 的新实例。
    /// </summary>
    /// <param name="broker">对应的 broker。</param>
    /// <param name="session">会话。</param>
    /// <param name="connection">连接。</param>
    internal MqttZeroConnectedClient(
        MqttZeroBroker broker,
        MqttZeroSessionState session,
        MqttZeroClientConnection connection)
    {
        _broker = broker;
        _session = session;
        _connection = connection;
    }

    /// <summary>获取客户端信息。</summary>
    public MqttZeroClientConnectionInfo ClientInfo
    {
        get
        {
            return _connection.ClientInfo;
        }
    }

    /// <summary>获取会话级的应用数据。</summary>
    public IDictionary<object, object?> Items
    {
        get
        {
            return _session.Items;
        }
    }

    /// <summary>获取该句柄是否仍然有效。</summary>
    public bool IsConnected
    {
        get
        {
            return MqttZeroBroker.IsConnectionAttached(_session, _connection);
        }
    }

    /// <summary>获取当前连接统计。</summary>
    public MqttZeroClientStatistics Statistics
    {
        get
        {
            return _connection.GetStatistics();
        }
    }

    /// <summary>让该客户端订阅请求的主题过滤器。</summary>
    public ValueTask<IReadOnlyList<MqttZeroSubscribeResult>> SubscribeAsync(
        IReadOnlyList<MqttSubscriptionRequest> subscriptions,
        uint? subscriptionIdentifier = null,
        CancellationToken cancellationToken = default)
    {
        return _broker.SubscribeConnectedClientAsync(
            _session,
            _connection,
            subscriptions,
            subscriptionIdentifier,
            cancellationToken);
    }

    /// <summary>让该客户端退订指定的主题过滤器。</summary>
    public ValueTask<IReadOnlyList<MqttZeroUnsubscribeResult>> UnsubscribeAsync(
        IReadOnlyList<string> topicFilters,
        CancellationToken cancellationToken = default)
    {
        return _broker.UnsubscribeConnectedClientAsync(
            _session,
            _connection,
            topicFilters,
            cancellationToken);
    }

    /// <summary>只向该客户端发送一条 PUBLISH 报文。</summary>
    public ValueTask<bool> TrySendPublishAsync(
        MqttZeroPublishMessage message,
        CancellationToken cancellationToken = default)
    {
        return _broker.SendPublishToClientAsync(
            _session,
            _connection,
            message,
            cancellationToken);
    }

    /// <summary>断开该客户端。</summary>
    public ValueTask<bool> DisconnectAsync(
        MqttReasonCode reasonCode = MqttReasonCode.Success,
        CancellationToken cancellationToken = default)
    {
        return _broker.DisconnectConnectedClientAsync(
            _session,
            _connection,
            reasonCode,
            cancellationToken);
    }

    /// <summary>获取客户端某一时刻的快照。</summary>
    public MqttZeroClientSnapshot GetSnapshot()
    {
        return MqttZeroBroker.CreateClientSnapshot(_session, _connection);
    }

}
