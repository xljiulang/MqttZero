using MqttZero.Server.Broker;
using MqttZero.Protocol;
using Microsoft.Extensions.DependencyInjection;

namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// 注册管理后台服务。
/// </summary>
public static class MqttZeroDashboardServiceCollectionExtensions
{
    /// <summary>
    /// Adds the optional management dashboard services.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">An optional dashboard configuration callback.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddMqttZeroDashboard(
        this IServiceCollection services,
        Action<MqttZeroDashboardOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions<MqttZeroDashboardOptions>();
        if (configure != null)
        {
            services.Configure(configure);
        }

        services.AddAntiforgery();
        return services;
    }
}
