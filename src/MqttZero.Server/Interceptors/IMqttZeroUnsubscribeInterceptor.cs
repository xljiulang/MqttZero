namespace MqttZero.Server.Interceptors;

/// <summary>
/// 授权单个退订过滤器的拦截器。
/// </summary>
public interface IMqttZeroUnsubscribeInterceptor
{
    /// <summary>拦截一个退订过滤器。</summary>
    ValueTask<MqttZeroInterceptionResult> InterceptAsync(
        MqttZeroUnsubscribeContext context,
        CancellationToken cancellationToken);
}
