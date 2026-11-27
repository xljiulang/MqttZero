namespace MqttZero.Server.Routing;

/// <summary>
/// 主题段的比较器。
/// </summary>
internal sealed class MqttZeroTopicSegmentComparer :
    IComparer<MqttZeroTopicSegment>
{
    /// <summary>
    /// Instance 的值。
    /// </summary>
    public static MqttZeroTopicSegmentComparer Instance { get; } = new();

    /// <summary>
    /// 比较。
    /// </summary>
    /// <param name="x">对应的 x。</param>
    /// <param name="y">对应的 y。</param>
    /// <returns>计算得到的数值。</returns>
    public int Compare(
        MqttZeroTopicSegment x,
        MqttZeroTopicSegment y)
    {
        return Compare(x.Span, y.Span);
    }

    /// <summary>
    /// 比较。
    /// </summary>
    /// <param name="x">对应的 x。</param>
    /// <param name="y">对应的 y。</param>
    /// <returns>计算得到的数值。</returns>
    public static int Compare(
        ReadOnlySpan<byte> x,
        ReadOnlySpan<byte> y)
    {
        return x.SequenceCompareTo(y);
    }
}
