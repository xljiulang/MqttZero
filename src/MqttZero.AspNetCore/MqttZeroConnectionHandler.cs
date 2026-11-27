using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Server.Interceptors;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.Extensions.Hosting;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;

namespace MqttZero.AspNetCore;

/// <summary>
/// 把 Kestrel 连接交给 Broker。
/// </summary>
public sealed class MqttZeroConnectionHandler : ConnectionHandler
{
    internal static readonly object EndpointOptionsKey = new();

    private readonly IHostApplicationLifetime _applicationLifetime;
    private readonly MqttZeroBroker _broker;

    /// <summary>
    /// Initializes a new connection handler.
    /// </summary>
    /// <param name="broker">The MQTTZero broker.</param>
    /// <param name="applicationLifetime">The host application lifetime.</param>
    public MqttZeroConnectionHandler(
        MqttZeroBroker broker,
        IHostApplicationLifetime applicationLifetime)
    {
        _broker = broker;
        _applicationLifetime = applicationLifetime;
    }

    /// <inheritdoc />
    public override async Task OnConnectedAsync(ConnectionContext connection)
    {
        using CancellationTokenSource cancellation = CancellationTokenSource.CreateLinkedTokenSource(
            connection.ConnectionClosed,
            _applicationLifetime.ApplicationStopping);

        ITransferFormatFeature? transferFormatFeature = connection.Features.Get<ITransferFormatFeature>();
        if (transferFormatFeature != null)
        {
            transferFormatFeature.ActiveFormat = TransferFormat.Binary;
        }

        Microsoft.AspNetCore.Http.HttpContext? httpContext = connection.GetHttpContext();
        bool isWebSocket = httpContext?.WebSockets.IsWebSocketRequest == true;
        bool isSecure = httpContext?.Request.IsHttps == true;
        MqttZeroTransportInfo transportInfo = new()
        {
            RemoteEndPoint = connection.RemoteEndPoint,
            IsSecure = isSecure,
            IsWebSocket = isWebSocket
        };
        MqttZeroEndpointOptions endpointOptions =
            connection.Items.TryGetValue(EndpointOptionsKey, out object? value) &&
            value is MqttZeroEndpointOptions configuredOptions
                ? configuredOptions
                : new MqttZeroEndpointOptions();
        IConnectionSocketFeature? socketFeature =
            connection.Features.Get<IConnectionSocketFeature>();
        if (socketFeature?.Socket is Socket socket)
        {
            endpointOptions.AcceptedSockets.Apply(socket, connection.RemoteEndPoint);
        }

        MqttZeroWebSocketOptions? webSocketOptions =
            httpContext?.GetEndpoint()?.Metadata.GetMetadata<MqttZeroWebSocketOptions>();
        bool allowPacketFragmentation = webSocketOptions != null
            ? webSocketOptions.ResolveAllowPacketFragmentation(transportInfo)
            : endpointOptions.ResolveAllowPacketFragmentation(transportInfo);
        X509Certificate2? clientCertificate =
            connection.Features.Get<ITlsConnectionFeature>()
                ?.ClientCertificate;

        MqttZeroTransportConnection transport = new()
        {
            Input = connection.Transport.Input,
            Output = connection.Transport.Output,
            RemoteEndPoint = connection.RemoteEndPoint,
            IsSecure = isSecure,
            IsWebSocket = isWebSocket,
            ClientCertificate = clientCertificate,
            AllowPacketFragmentation = allowPacketFragmentation
        };

        await _broker.AcceptConnectionAsync(
            transport,
            cancellation.Token).ConfigureAwait(false);
    }
}
