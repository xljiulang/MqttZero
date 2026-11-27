using MqttZero.Server.Broker;
using MqttZero.Protocol;
namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// 管理端展示的 MQTT 会话。
/// </summary>
public sealed record DashboardSessionDto(
    string ClientId,
    bool IsConnected,
    bool CleanStart,
    bool SessionPresent,
    uint ExpiryIntervalSeconds,
    DateTimeOffset? ExpiresAt,
    int SubscriptionCount,
    int PendingMessageCount,
    int InflightMessageCount,
    bool HasWill,
    string? WillTopic);
