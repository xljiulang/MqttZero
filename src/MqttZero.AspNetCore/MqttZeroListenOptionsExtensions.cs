using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;

namespace MqttZero.AspNetCore;

/// <summary>
/// 为 MQTTZero 配置 Kestrel 监听。
/// </summary>
public static class MqttZeroListenOptionsExtensions
{
    /// <summary>
    /// 让 Kestrel 监听使用 MQTTZero 的连接处理程序。
    /// </summary>
    /// <param name="listenOptions">The Kestrel listener options.</param>
    /// <param name="configure">An optional endpoint configuration callback.</param>
    /// <returns>The same listener options instance.</returns>
    public static ListenOptions UseMqttZero(
        this ListenOptions listenOptions,
        Action<MqttZeroEndpointOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(listenOptions);
        MqttZeroEndpointOptions endpointOptions = new();
        configure?.Invoke(endpointOptions);
        if (endpointOptions.Tls != null)
        {
            listenOptions.UseHttps(endpointOptions.Tls.CreateKestrelOptions());
        }

        listenOptions.Use(
            next => connection =>
            {
                connection.Items[MqttZeroConnectionHandler.EndpointOptionsKey] =
                    endpointOptions;
                return next(connection);
            });
        listenOptions.UseConnectionHandler<MqttZeroConnectionHandler>();
        return listenOptions;
    }
}
