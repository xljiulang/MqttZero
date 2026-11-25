using System.Text;

namespace MqttZero.Server.Scheduling;

internal sealed class MqttZeroSessionShardMap
{
    private const uint FnvOffsetBasis = 2166136261;
    private const uint FnvPrime = 16777619;
    private readonly int _shardCount;

    public MqttZeroSessionShardMap(int shardCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shardCount);
        _shardCount = shardCount;
    }

    public int GetShardId(string clientId)
    {
        ArgumentException.ThrowIfNullOrEmpty(clientId);
        byte[] utf8 = Encoding.UTF8.GetBytes(clientId);
        uint hash = FnvOffsetBasis;
        for (int index = 0; index < utf8.Length; index++)
        {
            hash ^= utf8[index];
            hash *= FnvPrime;
        }

        return (int)(hash % (uint)_shardCount);
    }
}
