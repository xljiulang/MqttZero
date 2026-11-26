using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using MqttZero.Server.Observers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace MqttZero.AspNetCore;

/// <summary>
/// Registers MQTTZero pipeline extensions.
/// </summary>
public static class MqttZeroBuilderInterceptorExtensions
{
    /// <summary>
    /// Registers an MQTT publish interceptor.
    /// </summary>
    /// <typeparam name="TInterceptor">The interceptor implementation.</typeparam>
    /// <param name="builder">The MQTTZero builder.</param>
    /// <returns>The same builder.</returns>
    public static IMqttZeroBuilder AddInterceptor<TInterceptor>(
        this IMqttZeroBuilder builder)
        where TInterceptor : class, IMqttZeroPublishInterceptor
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.TryAddSingleton<TInterceptor>();
        builder.Services.AddSingleton<IMqttZeroPublishInterceptor>(
            provider => provider.GetRequiredService<TInterceptor>());
        return builder;
    }

    /// <summary>Registers a CONNECT interceptor.</summary>
    public static IMqttZeroBuilder AddConnectionInterceptor<T>(this IMqttZeroBuilder builder)
        where T : class, IMqttZeroConnectionInterceptor
    {
        AddExtension<IMqttZeroConnectionInterceptor, T>(builder);
        return builder;
    }

    /// <summary>Registers a subscribe interceptor.</summary>
    public static IMqttZeroBuilder AddSubscribeInterceptor<T>(this IMqttZeroBuilder builder)
        where T : class, IMqttZeroSubscribeInterceptor
    {
        AddExtension<IMqttZeroSubscribeInterceptor, T>(builder);
        return builder;
    }

    /// <summary>Registers an unsubscribe interceptor.</summary>
    public static IMqttZeroBuilder AddUnsubscribeInterceptor<T>(this IMqttZeroBuilder builder)
        where T : class, IMqttZeroUnsubscribeInterceptor
    {
        AddExtension<IMqttZeroUnsubscribeInterceptor, T>(builder);
        return builder;
    }

    /// <summary>Registers an enqueue interceptor.</summary>
    public static IMqttZeroBuilder AddEnqueueInterceptor<T>(this IMqttZeroBuilder builder)
        where T : class, IMqttZeroEnqueueInterceptor
    {
        AddExtension<IMqttZeroEnqueueInterceptor, T>(builder);
        return builder;
    }

    /// <summary>Registers a connection observer.</summary>
    public static IMqttZeroBuilder AddConnectionObserver<T>(this IMqttZeroBuilder builder)
        where T : class, IMqttZeroConnectionObserver
    {
        AddExtension<IMqttZeroConnectionObserver, T>(builder);
        return builder;
    }

    /// <summary>Registers a delivery observer.</summary>
    public static IMqttZeroBuilder AddDeliveryObserver<T>(this IMqttZeroBuilder builder)
        where T : class, IMqttZeroDeliveryObserver
    {
        AddExtension<IMqttZeroDeliveryObserver, T>(builder);
        return builder;
    }

    private static void AddExtension<TService, TImplementation>(IMqttZeroBuilder builder)
        where TService : class
        where TImplementation : class, TService
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.TryAddSingleton<TImplementation>();
        builder.Services.AddSingleton<TService>(
            provider => provider.GetRequiredService<TImplementation>());
    }
}
