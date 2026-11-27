using MqttZero.Protocol;
using MqttZero.Server.Sessions;

namespace MqttZero.Server.Routing;

/// <summary>
/// 路由计划中的一个目标投递。
/// </summary>
internal struct MqttZeroPlannedDelivery
{
    private readonly MqttZeroRoutePlanBuffer _identifierSource;
    private int _identifierCount;
    private int _identifierOffset;
    private int _identifierWriteCount;
    private MqttQualityOfServiceLevel _qualityOfServiceLevel;
    private bool _retainAsPublished;

    /// <summary>
    /// 初始化 MqttZeroPlannedDelivery 的新实例。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="retainAsPublished">保留AsPublished。</param>
    /// <param name="identifierSource">标识符来源。</param>
    public MqttZeroPlannedDelivery(
        MqttZeroSessionState session,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retainAsPublished,
        MqttZeroRoutePlanBuffer identifierSource)
    {
        Session = session;
        _qualityOfServiceLevel = qualityOfServiceLevel;
        _retainAsPublished = retainAsPublished;
        _identifierSource = identifierSource;
        _identifierCount = 0;
        _identifierOffset = 0;
        _identifierWriteCount = 0;
    }

    /// <summary>
    /// 会话。
    /// </summary>
    public MqttZeroSessionState Session { get; }

    /// <summary>
    /// QualityOfServiceLevel 的值。
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel
    {
        get
        {
            return _qualityOfServiceLevel;
        }
    }

    /// <summary>
    /// RetainAsPublished 的值。
    /// </summary>
    public bool RetainAsPublished
    {
        get
        {
            return _retainAsPublished;
        }
    }

    /// <summary>
    /// SubscriptionIdentifiers 的值。
    /// </summary>
    public ReadOnlySpan<uint> SubscriptionIdentifiers
    {
        get
        {
            return _identifierSource.GetSubscriptionIdentifiers(
                _identifierOffset,
                _identifierCount);
        }
    }

    /// <summary>
    /// SubscriptionIdentifiersMemory 的值。
    /// </summary>
    internal ReadOnlyMemory<uint> SubscriptionIdentifiersMemory
    {
        get
        {
            return _identifierSource.GetSubscriptionIdentifiersMemory(
                _identifierOffset,
                _identifierCount);
        }
    }

    /// <summary>
    /// Increment标识符数量。
    /// </summary>
    internal void IncrementIdentifierCount()
    {
        _identifierCount++;
    }

    /// <summary>
    /// Prepare标识符偏移。
    /// </summary>
    /// <param name="currentOffset">当前偏移。</param>
    /// <returns>计算得到的数值。</returns>
    internal int PrepareIdentifierOffset(int currentOffset)
    {
        _identifierOffset = currentOffset;
        _identifierWriteCount = 0;
        return currentOffset + _identifierCount;
    }

    /// <summary>
    /// 合并Selection。
    /// </summary>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="retainAsPublished">保留AsPublished。</param>
    internal void MergeSelection(
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retainAsPublished)
    {
        if (qualityOfServiceLevel > _qualityOfServiceLevel)
        {
            _qualityOfServiceLevel = qualityOfServiceLevel;
            _retainAsPublished = retainAsPublished;
        }
    }

    /// <summary>
    /// 取出标识符写入索引。
    /// </summary>
    /// <returns>计算得到的数值。</returns>
    internal int TakeIdentifierWriteIndex()
    {
        int index = _identifierOffset + _identifierWriteCount;
        _identifierWriteCount++;
        return index;
    }
}
