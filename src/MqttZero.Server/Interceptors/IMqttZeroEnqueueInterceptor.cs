namespace MqttZero.Server.Interceptors;

/// <summary>
/// 控制投递到单个接收者的拦截器。
/// </summary>
public interface IMqttZeroEnqueueInterceptor
{
    /// <summary>拦截一次针对接收者的投递。</summary>
    ValueTask<MqttZeroEnqueueInterceptionResult> InterceptAsync(
        MqttZeroEnqueueContext context,
        CancellationToken cancellationToken);
}
