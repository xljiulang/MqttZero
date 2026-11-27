namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// 已连接客户端的分页结果。
/// </summary>
public sealed record DashboardClientPageDto(
    IReadOnlyList<DashboardClientDto> Items,
    int Page,
    int PageSize,
    int Total);
