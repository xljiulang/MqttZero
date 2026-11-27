using MqttZero.Server.Broker;
using MqttZero.Protocol;
namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// 管理端发起发布请求。
/// </summary>
public sealed class DashboardPublishRequest
{
    /// <summary>
    /// Gets or sets the MQTT topic.
    /// </summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the UTF-8 payload text.
    /// </summary>
    public string Payload { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the requested QoS level.
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the message is retained.
    /// </summary>
    public bool Retain { get; set; }
}
