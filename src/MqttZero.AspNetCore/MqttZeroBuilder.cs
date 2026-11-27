using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using Microsoft.Extensions.DependencyInjection;

namespace MqttZero.AspNetCore;

/// <summary>
/// MQTTZero 服务注册构建器。
/// </summary>
internal sealed class MqttZeroBuilder : IMqttZeroBuilder
{
    /// <summary>
    /// 初始化 MqttZeroBuilder 的新实例。
    /// </summary>
    /// <param name="services">服务。</param>
    public MqttZeroBuilder(IServiceCollection services)
    {
        Services = services;
    }

    /// <summary>
    /// 服务。
    /// </summary>
    public IServiceCollection Services { get; }
}
