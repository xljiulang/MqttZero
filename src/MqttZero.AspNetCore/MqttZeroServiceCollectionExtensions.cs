using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Scheduling;
using MqttZero.Server.Storage;
using Microsoft.AspNetCore.Server.Kestrel.Transport.Sockets;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace MqttZero.AspNetCore;

/// <summary>
/// Registers MQTTZero services.
/// </summary>
public static class MqttZeroServiceCollectionExtensions
{
    /// <summary>
    /// Adds the MQTTZero broker and Kestrel hosting services.
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

        services.AddOptions<SocketTransportOptions>()
            .Configure<IOptions<MqttZeroOptions>>(
                static (socketOptions, brokerOptions) =>
                {
                    socketOptions.IOQueueCount =
                        MqttZeroShardCountResolver.Resolve(
                            brokerOptions.Value.EventLoopShardCount);
                });
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
