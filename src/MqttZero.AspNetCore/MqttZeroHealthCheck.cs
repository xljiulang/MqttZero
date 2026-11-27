using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace MqttZero.AspNetCore;

/// <summary>
/// 报告 Broker 是否就绪。
/// </summary>
public sealed class MqttZeroHealthCheck : IHealthCheck
{
    private readonly IMqttZeroBroker _broker;

    /// <summary>
    /// 初始化健康检查的新实例。
    /// </summary>
    /// <param name="broker">The MQTTZero broker.</param>
    public MqttZeroHealthCheck(IMqttZeroBroker broker)
    {
        _broker = broker;
    }

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        MqttZeroBrokerState state = _broker.State;
        return Task.FromResult(
            state == MqttZeroBrokerState.Running
                ? HealthCheckResult.Healthy("MQTTZero broker is running.")
                : HealthCheckResult.Unhealthy($"MQTTZero broker state is {state}."));
    }
}
