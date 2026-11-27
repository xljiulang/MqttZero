namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// 会话的分页结果。
/// </summary>
public sealed record DashboardSessionPageDto(
    IReadOnlyList<DashboardSessionDto> Items,
    int Page,
    int PageSize,
    int Total);
