using MqttZero.Server.Broker;
using MqttZero.Protocol;
namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// 管理端总览数据。
/// </summary>
public sealed record DashboardOverviewDto(
    string State,
    int ActiveConnections,
    int ConnectedClients);
