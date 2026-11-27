using MqttZero.Memory;
using MqttZero.Protocol;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace MqttZero.Server.Sessions;

/// <summary>
/// 会话中的一条订阅。
/// </summary>
internal sealed class MqttZeroSubscription
{
    private static readonly byte[] SharedPrefix = "$share/"u8.ToArray();

    /// <summary>
    /// 初始化 MqttZeroSubscription 的新实例。
    /// </summary>
    /// <param name="topicFilter">主题过滤器。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="noLocal">no本地。</param>
    /// <param name="retainAsPublished">保留AsPublished。</param>
    /// <param name="subscriptionIdentifier">订阅标识符。</param>
    public MqttZeroSubscription(
        byte[] topicFilter,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool noLocal,
        bool retainAsPublished,
        uint? subscriptionIdentifier)
    {
        TopicFilter = topicFilter;
        QualityOfServiceLevel = qualityOfServiceLevel;
        NoLocal = noLocal;
        RetainAsPublished = retainAsPublished;
        SubscriptionIdentifier = subscriptionIdentifier;
        SubscriptionIdentifiers = subscriptionIdentifier.HasValue
            ? new[] { subscriptionIdentifier.Value }
            : ReadOnlyMemory<uint>.Empty;

        if (topicFilter.AsSpan().StartsWith(SharedPrefix))
        {
            int groupStart = SharedPrefix.Length;
            int groupEnd = topicFilter.AsSpan(groupStart).IndexOf((byte)'/');
            if (groupEnd > 0)
            {
                IsShared = true;
                SharedGroup = Encoding.UTF8.GetString(topicFilter, groupStart, groupEnd);
                MatchFilter = topicFilter[(groupStart + groupEnd + 1)..];
                return;
            }
        }

        MatchFilter = topicFilter;
    }

    /// <summary>
    /// 主题过滤器。
    /// </summary>
    public byte[] TopicFilter { get; }

    /// <summary>
    /// 服务质量ServiceLevel。
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; }

    /// <summary>
    /// No本地。
    /// </summary>
    public bool NoLocal { get; }

    /// <summary>
    /// 保留AsPublished。
    /// </summary>
    public bool RetainAsPublished { get; }

    /// <summary>
    /// 订阅标识符。
    /// </summary>
    public uint? SubscriptionIdentifier { get; }

    /// <summary>
    /// 订阅标识符。
    /// </summary>
    public ReadOnlyMemory<uint> SubscriptionIdentifiers { get; }

    /// <summary>
    /// 判断是否共享。
    /// </summary>
    public bool IsShared { get; }

    /// <summary>
    /// 共享组。
    /// </summary>
    public string? SharedGroup { get; }

    /// <summary>
    /// 匹配过滤器。
    /// </summary>
    public byte[] MatchFilter { get; }

    /// <summary>
    /// Gets the shared group name when this is a shared subscription.
    /// </summary>
    /// <param name="sharedGroup">The shared group name when shared.</param>
    /// <returns><see langword="true"/> when the subscription is shared.</returns>
    public bool TryGetSharedGroup(
        [NotNullWhen(true)] out string? sharedGroup)
    {
        sharedGroup = SharedGroup;
        return IsShared;
    }
}
