namespace MqttZero.Server.Interceptors;

/// <summary>
/// 控制投递到单个接收者的拦截器。
/// </summary>
public interface IMqttZeroEnqueueInterceptor
{
    /// <summary>Intercepts one receiver delivery.</summary>
    ValueTask<MqttZeroEnqueueInterceptionResult> InterceptAsync(
        MqttZeroEnqueueContext context,
        CancellationToken cancellationToken);
}
