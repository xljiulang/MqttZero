namespace MqttZero.Server.Interceptors;

/// <summary>
/// 授权单个订阅过滤器的拦截器。
/// </summary>
public interface IMqttZeroSubscribeInterceptor
{
    /// <summary>拦截一个订阅过滤器。</summary>
    ValueTask<MqttZeroInterceptionResult> InterceptAsync(
        MqttZeroSubscribeContext context,
        CancellationToken cancellationToken);
}
