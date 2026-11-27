using MqttZero.Server.Broker;
using MqttZero.Protocol;
using System.Text.Json.Serialization;

namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// 管理后台 DTO 的源生成 JSON 元数据。
/// </summary>
[JsonSerializable(typeof(DashboardOverviewDto))]
[JsonSerializable(typeof(DashboardClientDto[]))]
[JsonSerializable(typeof(DashboardSessionDto[]))]
[JsonSerializable(typeof(DashboardClientPageDto))]
[JsonSerializable(typeof(DashboardSessionPageDto))]
[JsonSerializable(typeof(DashboardPublishRequest))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
public partial class DashboardJsonSerializerContext : JsonSerializerContext
{
}
