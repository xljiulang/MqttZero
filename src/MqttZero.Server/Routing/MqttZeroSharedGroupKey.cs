using MqttZero.Server.Sessions;
using System.Text;

namespace MqttZero.Server.Routing;

/// <summary>
/// 由组名与过滤器组成的共享组键。
/// </summary>
internal readonly struct MqttZeroSharedGroupKey :
    IEquatable<MqttZeroSharedGroupKey>
{
    private readonly byte[] _group;
    private readonly byte[] _filter;

    /// <summary>
    /// 初始化 MqttZeroSharedGroupKey 的新实例。
    /// </summary>
    /// <param name="group">组。</param>
    /// <param name="filter">过滤器。</param>
    public MqttZeroSharedGroupKey(
        ReadOnlySpan<byte> group,
        ReadOnlySpan<byte> filter)
    {
        _group = group.ToArray();
        _filter = filter.ToArray();
    }

    /// <summary>
    /// 创建共享订阅的键。
    /// </summary>
    /// <param name="subscription">The shared subscription.</param>
    /// <returns>The shared group key.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown when the subscription is not a shared subscription.
    /// </exception>
    public static MqttZeroSharedGroupKey Create(
        MqttZeroSubscription subscription)
    {
        ArgumentNullException.ThrowIfNull(subscription);
        if (!subscription.TryGetSharedGroup(out string? sharedGroup))
        {
            throw new ArgumentException(
                "The subscription is not a shared subscription.",
                nameof(subscription));
        }

        return new MqttZeroSharedGroupKey(
            Encoding.UTF8.GetBytes(sharedGroup),
            subscription.MatchFilter);
    }

    /// <summary>
    /// Group 的值。
    /// </summary>
    public ReadOnlySpan<byte> Group
    {
        get
        {
            return _group;
        }
    }

    /// <summary>
    /// Filter 的值。
    /// </summary>
    public ReadOnlySpan<byte> Filter
    {
        get
        {
            return _filter;
        }
    }

    /// <summary>
    /// 执行 Equals 对应的处理。
    /// </summary>
    /// <param name="other">对应的 other。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public bool Equals(MqttZeroSharedGroupKey other)
    {
        return Group.SequenceEqual(other.Group) &&
            Filter.SequenceEqual(other.Filter);
    }

    /// <summary>
    /// 执行 Equals 对应的处理。
    /// </summary>
    /// <param name="obj">对应的 obj。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public override bool Equals(object? obj)
    {
        return obj is MqttZeroSharedGroupKey other && Equals(other);
    }

    /// <summary>
    /// 获取Hash代码。
    /// </summary>
    /// <returns>计算得到的数值。</returns>
    public override int GetHashCode()
    {
        uint hash = 2166136261;
        for (int index = 0; index < _group.Length; index++)
        {
            hash = (hash ^ _group[index]) * 16777619;
        }

        hash = (hash ^ 0xFF) * 16777619;
        for (int index = 0; index < _filter.Length; index++)
        {
            hash = (hash ^ _filter[index]) * 16777619;
        }

        return (int)hash;
    }
}
