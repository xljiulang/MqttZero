using MqttZero.Server.Scheduling;

namespace MqttZero.Tests.Scheduling;

public sealed class MqttZeroShardSetTests
{
    [Fact]
    public async Task CreatesConfiguredNumberOfShards()
    {
        await using MqttZeroShardSet shards = new(4, 32);

        Assert.Equal(4, shards.Count);
        Assert.Equal([0, 1, 2, 3], Enumerable.Range(0, shards.Count));
        Assert.Equal(
            4,
            shards.GetSnapshot().Shards.Count);
    }

    [Fact(Timeout = 5_000)]
    public async Task StopsAndDisposesIdempotently()
    {
        MqttZeroShardSet shards = new(2, 32);

        await shards.StopAsync();
        await shards.StopAsync();
        await shards.DisposeAsync();
        await shards.DisposeAsync();
        _ = TestContext.Current.CancellationToken;
    }

    [Fact]
    public void RejectsInvalidConfiguration()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new MqttZeroShardSet(0, 32));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new MqttZeroShardSet(1, 0));
    }

    [Fact]
    public async Task GetsStableSessionShard()
    {
        await using MqttZeroShardSet shards = new(8, 32);

        MqttZeroEventLoop first = shards.GetSessionShard("device/one");
        MqttZeroEventLoop second = shards.GetSessionShard("device/one");

        Assert.Same(first, second);
    }
}
