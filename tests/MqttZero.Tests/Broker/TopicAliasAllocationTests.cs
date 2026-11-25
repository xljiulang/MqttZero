using MqttZero.Memory;
using MqttZero.Server.Clients;
using System.Buffers;

namespace MqttZero.Tests.Broker;

public sealed class TopicAliasAllocationTests
{
    [Fact]
    public void CreatingAliasTableDoesNotAllocateFixedAliasArray()
    {
        using MqttZeroBufferPool pool = new(1024 * 1024);
        long before = GC.GetAllocatedBytesForCurrentThread();

        _ = new MqttZeroTopicAliasTable(pool);

        long allocated =
            GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.InRange(allocated, 0, 1024);
    }

    [Fact]
    public void AliasTableStoresOverwritesAndReleasesAliases()
    {
        using MqttZeroBufferPool pool = new(1024 * 1024);
        MqttZeroTopicAliasTable table = new(pool);
        table.Set(
            1,
            new ReadOnlySequence<byte>("devices/a"u8.ToArray()));
        Assert.True(table.TryGet(1, out ReadOnlyMemory<byte> first));
        Assert.Equal("devices/a", System.Text.Encoding.UTF8.GetString(
            first.Span));

        table.Set(
            1,
            new ReadOnlySequence<byte>(
                "devices/replacement"u8.ToArray()));
        Assert.True(table.TryGet(1, out ReadOnlyMemory<byte> second));
        Assert.Equal("devices/replacement",
            System.Text.Encoding.UTF8.GetString(second.Span));

        table.Set(
            300,
            new ReadOnlySequence<byte>("large/alias"u8.ToArray()));
        Assert.True(table.TryGet(300, out ReadOnlyMemory<byte> large));
        Assert.Equal("large/alias",
            System.Text.Encoding.UTF8.GetString(large.Span));

        table.Dispose();
        Assert.Equal(0, pool.RentedBytes);
    }
}
