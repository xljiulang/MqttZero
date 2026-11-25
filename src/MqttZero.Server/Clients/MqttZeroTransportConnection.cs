using System.IO.Pipelines;
using System.Net;

namespace MqttZero.Server.Clients;

internal sealed class MqttZeroTransportConnection
{
    public MqttZeroTransportConnection(
        PipeReader input,
        PipeWriter output,
        EndPoint? remoteEndPoint,
        bool isSecure,
        bool isWebSocket,
        bool allowPacketFragmentation)
    {
        Input = input ?? throw new ArgumentNullException(nameof(input));
        Output = output ?? throw new ArgumentNullException(nameof(output));
        RemoteEndPoint = remoteEndPoint;
        IsSecure = isSecure;
        IsWebSocket = isWebSocket;
        AllowPacketFragmentation = allowPacketFragmentation;
    }

    public PipeReader Input { get; }

    public PipeWriter Output { get; }

    public EndPoint? RemoteEndPoint { get; }

    public bool IsSecure { get; }

    public bool IsWebSocket { get; }

    public bool AllowPacketFragmentation { get; }
}
