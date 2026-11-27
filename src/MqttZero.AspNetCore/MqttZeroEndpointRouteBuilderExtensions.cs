using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.Routing;

namespace MqttZero.AspNetCore;

/// <summary>
/// 映射 MQTT over WebSocket 端点。
/// </summary>
public static class MqttZeroEndpointRouteBuilderExtensions
{
    /// <summary>
    /// 映射 MQTT over WebSocket 端点。
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <param name="pattern">The endpoint route pattern.</param>
    /// <param name="configure">An optional WebSocket endpoint configuration callback.</param>
    /// <returns>The mapped connection endpoint.</returns>
    public static IEndpointConventionBuilder MapMqttZero(
        this IEndpointRouteBuilder endpoints,
        string pattern,
        Action<MqttZeroWebSocketOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentException.ThrowIfNullOrWhiteSpace(pattern);

        MqttZeroWebSocketOptions webSocketOptions = new();
        configure?.Invoke(webSocketOptions);
        IEndpointConventionBuilder builder = endpoints.MapConnectionHandler<MqttZeroConnectionHandler>(
            pattern,
            static options =>
            {
                options.WebSockets.SubProtocolSelector = SelectMqttSubProtocol;
            });
        builder.WithMetadata(webSocketOptions);
        return builder;
    }

    /// <summary>
    /// 选择MqttSubProtocol。
    /// </summary>
    /// <param name="requestedProtocols">对应的 requestedProtocols。</param>
    /// <returns>对应的字符串。</returns>
    private static string SelectMqttSubProtocol(IList<string> requestedProtocols)
    {
        foreach (string protocol in requestedProtocols)
        {
            if (protocol.Equals("mqtt", StringComparison.OrdinalIgnoreCase))
            {
                return protocol;
            }
        }

        return string.Empty;
    }
}
