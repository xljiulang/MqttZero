using MqttZero.Server.Broker;
using MqttZero.Protocol;
namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// 管理后台的配置。
/// </summary>
public sealed class MqttZeroDashboardOptions
{
    /// <summary>
    /// Gets or sets the authorization policy required for read-only dashboard access.
    /// </summary>
    public string ReadAuthorizationPolicy { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the authorization policy required for management operations.
    /// </summary>
    public string OperateAuthorizationPolicy { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the default page size for list APIs.
    /// </summary>
    public int DefaultPageSize { get; set; } = 50;

    /// <summary>
    /// Gets or sets the maximum page size for list APIs.
    /// </summary>
    public int MaxPageSize { get; set; } = 200;
}
