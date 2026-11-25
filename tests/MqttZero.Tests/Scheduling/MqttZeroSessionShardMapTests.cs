using MqttZero.Server.Scheduling;

namespace MqttZero.Tests.Scheduling;

public sealed class MqttZeroSessionShardMapTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(8)]
    [InlineData(16)]
    public void SameClientIdAlwaysMapsToSameShard(int shardCount)
    {
        MqttZeroSessionShardMap map = new(shardCount);

        int first = map.GetShardId("device/客户端-01");
        int second = map.GetShardId("device/客户端-01");

        Assert.Equal(first, second);
        Assert.InRange(first, 0, shardCount - 1);
    }

    [Fact]
    public void DifferentClientIdsUseMultipleShards()
    {
        MqttZeroSessionShardMap map = new(8);
        HashSet<int> observed = [];

        for (int index = 0; index < 100; index++)
        {
            observed.Add(map.GetShardId($"client-{index}"));
        }

        Assert.True(observed.Count > 1);
    }

    [Fact]
    public void LongClientIdUsesStablePooledUtf8Path()
    {
        MqttZeroSessionShardMap map = new(8);
        string clientId = new(
            '客',
            200);

        int first = map.GetShardId(clientId);
        int second = map.GetShardId(clientId);

        Assert.Equal(first, second);
        Assert.InRange(first, 0, 7);
    }

    [Fact]
    public void RejectsInvalidShardCount()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new MqttZeroSessionShardMap(0));
    }
}
