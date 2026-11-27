using MqttZero.Server.Broker;
using MqttZero.Protocol;
namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// 管理端展示的已连接客户端。
/// </summary>
public sealed record DashboardClientDto(
    string ClientId,
    string? RemoteEndPoint,
    bool IsSecure,
    bool IsWebSocket,
    int SubscriptionCount);
