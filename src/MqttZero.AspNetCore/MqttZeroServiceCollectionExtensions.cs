using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace MqttZero.AspNetCore;

/// <summary>
/// 注册 MQTTZero 服务。
/// </summary>
public static class MqttZeroServiceCollectionExtensions
{
    /// <summary>
    /// 注册 MQTTZero Broker 与 Kestrel 宿主服务。
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">An optional broker configuration callback.</param>
    /// <returns>A builder for additional MQTTZero configuration.</returns>
    public static IMqttZeroBuilder AddMqttZero(
        this IServiceCollection services,
        Action<MqttZeroOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddLogging();
        services.AddOptions();
        services.AddConnections();
        if (configure != null)
        {
            services.Configure(configure);
        }

        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IMqttZeroSessionStore, MqttZeroNoopSessionStore>();
        services.TryAddSingleton<IMqttZeroMessageStore, MqttZeroNoopMessageStore>();
        services.TryAddSingleton<IMqttZeroRetainedMessageStore, MqttZeroNoopRetainedMessageStore>();
        services.TryAddSingleton<MqttZeroBroker>();
        services.TryAddSingleton<IMqttZeroBroker>(
            static provider => provider.GetRequiredService<MqttZeroBroker>());
        services.TryAddSingleton<MqttZeroConnectionHandler>();
        services.AddHostedService<MqttZeroHostedService>();

        return new MqttZeroBuilder(services);
    }
}
