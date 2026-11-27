using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using MqttZero.Server.Observers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace MqttZero.AspNetCore;

/// <summary>
/// 注册 MQTTZero 的管线扩展。
/// </summary>
public static class MqttZeroBuilderInterceptorExtensions
{
    /// <summary>
    /// 注册 MQTT 发布拦截器。
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

    /// <summary>注册 CONNECT 拦截器。</summary>
    public static IMqttZeroBuilder AddConnectionInterceptor<T>(this IMqttZeroBuilder builder)
        where T : class, IMqttZeroConnectionInterceptor
    {
        AddExtension<IMqttZeroConnectionInterceptor, T>(builder);
        return builder;
    }

    /// <summary>注册订阅拦截器。</summary>
    public static IMqttZeroBuilder AddSubscribeInterceptor<T>(this IMqttZeroBuilder builder)
        where T : class, IMqttZeroSubscribeInterceptor
    {
        AddExtension<IMqttZeroSubscribeInterceptor, T>(builder);
        return builder;
    }

    /// <summary>注册退订拦截器。</summary>
    public static IMqttZeroBuilder AddUnsubscribeInterceptor<T>(this IMqttZeroBuilder builder)
        where T : class, IMqttZeroUnsubscribeInterceptor
    {
        AddExtension<IMqttZeroUnsubscribeInterceptor, T>(builder);
        return builder;
    }

    /// <summary>注册入队拦截器。</summary>
    public static IMqttZeroBuilder AddEnqueueInterceptor<T>(this IMqttZeroBuilder builder)
        where T : class, IMqttZeroEnqueueInterceptor
    {
        AddExtension<IMqttZeroEnqueueInterceptor, T>(builder);
        return builder;
    }

    /// <summary>注册连接观察者。</summary>
    public static IMqttZeroBuilder AddConnectionObserver<T>(this IMqttZeroBuilder builder)
        where T : class, IMqttZeroConnectionObserver
    {
        AddExtension<IMqttZeroConnectionObserver, T>(builder);
        return builder;
    }

    /// <summary>注册投递观察者。</summary>
    public static IMqttZeroBuilder AddDeliveryObserver<T>(this IMqttZeroBuilder builder)
        where T : class, IMqttZeroDeliveryObserver
    {
        AddExtension<IMqttZeroDeliveryObserver, T>(builder);
        return builder;
    }

    /// <summary>
    /// 添加Extension。
    /// </summary>
    /// <param name="builder">构建器。</param>
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
