using MqttZero.Server.Clients;
using System.IO.Pipelines;
using System.Net;

namespace MqttZero.Tests.Broker;

public sealed class MqttZeroTransportConnectionTests
{
    [Fact]
    public void ConstructorPreservesTransportDetails()
    {
        Pipe input = new();
        Pipe output = new();
        IPEndPoint remoteEndPoint = new(IPAddress.Loopback, 1883);

        MqttZeroTransportConnection connection = new(
            input.Reader,
            output.Writer,
            remoteEndPoint,
            isSecure: true,
            isWebSocket: true,
            allowPacketFragmentation: false);

        Assert.Same(input.Reader, connection.Input);
        Assert.Same(output.Writer, connection.Output);
        Assert.Same(remoteEndPoint, connection.RemoteEndPoint);
        Assert.True(connection.IsSecure);
        Assert.True(connection.IsWebSocket);
        Assert.False(connection.AllowPacketFragmentation);
    }

    [Fact]
    public void ConstructorRejectsMissingPipes()
    {
        Pipe pipe = new();

        Assert.Throws<ArgumentNullException>(() =>
            new MqttZeroTransportConnection(
                null!,
                pipe.Writer,
                null,
                isSecure: false,
                isWebSocket: false,
                allowPacketFragmentation: true));
        Assert.Throws<ArgumentNullException>(() =>
            new MqttZeroTransportConnection(
                pipe.Reader,
                null!,
                null,
                isSecure: false,
                isWebSocket: false,
                allowPacketFragmentation: true));
    }
}
