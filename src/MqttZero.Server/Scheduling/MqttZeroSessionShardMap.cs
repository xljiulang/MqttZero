using System.Buffers;
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
        int maximumByteCount = Encoding.UTF8.GetMaxByteCount(
            clientId.Length);
        if (maximumByteCount <= 256)
        {
            return GetShardIdFromStack(
                clientId,
                maximumByteCount);
        }

        return GetShardIdFromPool(clientId, maximumByteCount);
    }

    private int GetShardIdFromStack(
        string clientId,
        int maximumByteCount)
    {
        Span<byte> utf8 = stackalloc byte[maximumByteCount];
        int byteCount = Encoding.UTF8.GetBytes(clientId, utf8);
        return Hash(utf8[..byteCount]);
    }

    private int GetShardIdFromPool(
        string clientId,
        int maximumByteCount)
    {
        byte[] rentedBuffer = ArrayPool<byte>.Shared.Rent(
            maximumByteCount);

        try
        {
            int byteCount = Encoding.UTF8.GetBytes(
                clientId,
                rentedBuffer);
            return Hash(rentedBuffer.AsSpan(0, byteCount));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rentedBuffer);
        }
    }

    private int Hash(ReadOnlySpan<byte> utf8)
    {
        uint hash = FnvOffsetBasis;
        for (int index = 0; index < utf8.Length; index++)
        {
            hash ^= utf8[index];
            hash *= FnvPrime;
        }

        return (int)(hash % (uint)_shardCount);
    }
}
