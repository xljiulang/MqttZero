using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using Microsoft.Extensions.DependencyInjection;

namespace MqttZero.AspNetCore;

/// <summary>
/// 服务注册能力接口。
/// </summary>
public interface IMqttZeroBuilder
{
    /// <summary>
    /// Gets the service collection used by the builder.
    /// </summary>
    IServiceCollection Services { get; }
}
