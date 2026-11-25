using Microsoft.AspNetCore.Server.Kestrel.Transport.Sockets;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MqttZero.AspNetCore;

namespace MqttZero.Tests.AspNetCore;

public sealed class MqttZeroShardIoQueueTests
{
    [Fact]
    public void AddMqttZeroAlignsKestrelIoQueues()
    {
        ServiceCollection services = new();
        services.AddMqttZero(options =>
        {
            options.EventLoopShardCount = 4;
        });
        using ServiceProvider provider = services.BuildServiceProvider();

        SocketTransportOptions options =
            provider.GetRequiredService<IOptions<SocketTransportOptions>>()
                .Value;

        Assert.Equal(4, options.IOQueueCount);
    }

    [Fact]
    public void LaterExplicitIoQueueConfigurationWins()
    {
        ServiceCollection services = new();
        services.AddMqttZero(options =>
        {
            options.EventLoopShardCount = 4;
        });
        services.Configure<SocketTransportOptions>(options =>
        {
            options.IOQueueCount = 3;
        });
        using ServiceProvider provider = services.BuildServiceProvider();

        SocketTransportOptions options =
            provider.GetRequiredService<IOptions<SocketTransportOptions>>()
                .Value;

        Assert.Equal(3, options.IOQueueCount);
    }
}
