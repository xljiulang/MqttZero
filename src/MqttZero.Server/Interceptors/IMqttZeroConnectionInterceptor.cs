namespace MqttZero.Server.Interceptors;

/// <summary>
/// 在会话变更前校验 CONNECT 的拦截器。
/// </summary>
public interface IMqttZeroConnectionInterceptor
{
    /// <summary>拦截一个 CONNECT 请求。</summary>
    ValueTask<MqttZeroConnectionInterceptionResult> InterceptAsync(
        MqttZeroConnectionContext context,
        CancellationToken cancellationToken);
}
