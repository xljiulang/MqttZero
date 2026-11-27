using MqttZero.Server.Broker;
using MqttZero.Protocol;
namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// 管理端发起发布请求。
/// </summary>
public sealed class DashboardPublishRequest
{
    /// <summary>
    /// 获取或设置 MQTT 主题。
    /// </summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>
    /// 获取或设置 UTF-8 负载文本。
    /// </summary>
    public string Payload { get; set; } = string.Empty;

    /// <summary>
    /// 获取或设置请求的服务质量等级。
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; set; }

    /// <summary>
    /// 获取或设置消息是否作为保留消息发布。
    /// </summary>
    public bool Retain { get; set; }
}
