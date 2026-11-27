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
    /// 获取构建器使用的服务集合。
    /// </summary>
    IServiceCollection Services { get; }
}
