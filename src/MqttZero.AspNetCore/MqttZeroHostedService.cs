using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using Microsoft.Extensions.Hosting;

namespace MqttZero.AspNetCore;

/// <summary>
/// Broker 的宿主生命周期服务。
/// </summary>
internal sealed class MqttZeroHostedService : IHostedService
{
    private readonly MqttZeroBroker _broker;

    /// <summary>
    /// 初始化 MqttZeroHostedService 的新实例。
    /// </summary>
    /// <param name="broker">对应的 broker。</param>
    public MqttZeroHostedService(MqttZeroBroker broker)
    {
        _broker = broker;
    }

    /// <summary>
    /// 启动。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        return _broker.StartAsync(cancellationToken).AsTask();
    }

    /// <summary>
    /// 停止。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public Task StopAsync(CancellationToken cancellationToken)
    {
        return _broker.StopAsync(cancellationToken).AsTask();
    }
}
