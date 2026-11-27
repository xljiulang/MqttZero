using MqttZero.Server.Broker;
using MqttZero.Protocol;
namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// 管理后台的配置。
/// </summary>
public sealed class MqttZeroDashboardOptions
{
    /// <summary>
    /// 获取或设置只读看板访问所需的授权策略。
    /// </summary>
    public string ReadAuthorizationPolicy { get; set; } = string.Empty;

    /// <summary>
    /// 获取或设置管理操作所需的授权策略。
    /// </summary>
    public string OperateAuthorizationPolicy { get; set; } = string.Empty;

    /// <summary>
    /// 获取或设置列表 API 的默认分页大小。
    /// </summary>
    public int DefaultPageSize { get; set; } = 50;

    /// <summary>
    /// 获取或设置列表 API 的最大分页大小。
    /// </summary>
    public int MaxPageSize { get; set; } = 200;
}
