using MqttZero.Protocol;
namespace MqttZero.Server.Interceptors;

/// <summary>
/// 在路由前拦截发布的拦截器。
/// </summary>
public interface IMqttZeroPublishInterceptor
{
    /// <summary>
    /// Intercepts one publish operation.
    /// </summary>
    /// <param name="context">The mutable publish context.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>The continuation decision.</returns>
    ValueTask<MqttZeroInterceptionResult> InterceptAsync(
        MqttZeroPublishContext context,
        CancellationToken cancellationToken);
}
