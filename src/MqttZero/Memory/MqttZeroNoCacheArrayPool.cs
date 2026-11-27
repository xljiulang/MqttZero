using System.Buffers;

namespace MqttZero.Memory;

/// <summary>
/// 不保留缓存的 ArrayPool 适配器。
/// </summary>
internal sealed class MqttZeroNoCacheArrayPool : ArrayPool<byte>
{
    /// <summary>
    /// Instance 的值。
    /// </summary>
    public static MqttZeroNoCacheArrayPool Instance { get; } = new();

    /// <summary>
    /// 初始化 MqttZeroNoCacheArrayPool 的新实例。
    /// </summary>
    private MqttZeroNoCacheArrayPool()
    {
    }

    /// <summary>
    /// 租用。
    /// </summary>
    /// <param name="minimumLength">最小长度。</param>
    /// <returns>字节数组。</returns>
    public override byte[] Rent(int minimumLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(minimumLength);
        return new byte[minimumLength];
    }

    /// <summary>
    /// 归还。
    /// </summary>
    /// <param name="array">对应的 array。</param>
    /// <param name="clearArray">对应的 clearArray。</param>
    public override void Return(byte[] array, bool clearArray = false)
    {
        if (clearArray)
        {
            Array.Clear(array);
        }
    }
}
