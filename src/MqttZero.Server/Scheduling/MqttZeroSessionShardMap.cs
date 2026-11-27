using System.Buffers;
using System.Text;

namespace MqttZero.Server.Scheduling;

/// <summary>
/// Client ID 到分片的稳定映射。
/// </summary>
internal sealed class MqttZeroSessionShardMap
{
    private const uint FnvOffsetBasis = 2166136261;
    private const uint FnvPrime = 16777619;
    private readonly int _shardCount;

    /// <summary>
    /// 初始化 MqttZeroSessionShardMap 的新实例。
    /// </summary>
    /// <param name="shardCount">分片数量。</param>
    public MqttZeroSessionShardMap(int shardCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shardCount);
        _shardCount = shardCount;
    }

    /// <summary>
    /// 获取分片Id。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <returns>计算得到的数值。</returns>
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

    /// <summary>
    /// 获取分片IdFromStack。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="maximumByteCount">最大字节数量。</param>
    /// <returns>计算得到的数值。</returns>
    private int GetShardIdFromStack(
        string clientId,
        int maximumByteCount)
    {
        Span<byte> utf8 = stackalloc byte[maximumByteCount];
        int byteCount = Encoding.UTF8.GetBytes(clientId, utf8);
        return Hash(utf8[..byteCount]);
    }

    /// <summary>
    /// 获取分片IdFrom池。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="maximumByteCount">最大字节数量。</param>
    /// <returns>计算得到的数值。</returns>
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

    /// <summary>
    /// 执行 Hash 对应的处理。
    /// </summary>
    /// <param name="utf8">UTF-8。</param>
    /// <returns>计算得到的数值。</returns>
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
