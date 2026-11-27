namespace MqttZero.Server.Routing;

/// <summary>
/// 主题中的一个段。
/// </summary>
internal readonly struct MqttZeroTopicSegment : IEquatable<MqttZeroTopicSegment>
{
    private readonly byte[] _bytes;

    /// <summary>
    /// 初始化 MqttZeroTopicSegment 的新实例。
    /// </summary>
    /// <param name="bytes">字节数。</param>
    private MqttZeroTopicSegment(byte[] bytes)
    {
        _bytes = bytes;
    }

    /// <summary>
    /// Span 的值。
    /// </summary>
    public ReadOnlySpan<byte> Span
    {
        get
        {
            return _bytes;
        }
    }

    /// <summary>
    /// 从。
    /// </summary>
    /// <param name="value">值。</param>
    /// <returns>对应的结果。</returns>
    public static MqttZeroTopicSegment From(ReadOnlySpan<byte> value)
    {
        return new MqttZeroTopicSegment(value.ToArray());
    }

    /// <summary>
    /// 执行 Equals 对应的处理。
    /// </summary>
    /// <param name="other">对应的 other。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public bool Equals(MqttZeroTopicSegment other)
    {
        return Span.SequenceEqual(other.Span);
    }

    /// <summary>
    /// 执行 Equals 对应的处理。
    /// </summary>
    /// <param name="obj">对应的 obj。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public override bool Equals(object? obj)
    {
        return obj is MqttZeroTopicSegment other && Equals(other);
    }

    /// <summary>
    /// 获取Hash代码。
    /// </summary>
    /// <returns>计算得到的数值。</returns>
    public override int GetHashCode()
    {
        uint hash = 2166136261;
        for (int index = 0; index < _bytes.Length; index++)
        {
            hash = (hash ^ _bytes[index]) * 16777619;
        }

        return (int)hash;
    }
}
