using MqttZero.Protocol;
using MqttZero.Server.Sessions;

namespace MqttZero.Server.Routing;

internal struct MqttZeroPlannedDelivery
{
    private readonly MqttZeroRoutePlanBuffer _identifierSource;
    private int _identifierCount;
    private int _identifierOffset;
    private int _identifierWriteCount;
    private MqttQualityOfServiceLevel _qualityOfServiceLevel;
    private bool _retainAsPublished;

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

    public MqttZeroSessionState Session { get; }

    public MqttQualityOfServiceLevel QualityOfServiceLevel
    {
        get
        {
            return _qualityOfServiceLevel;
        }
    }

    public bool RetainAsPublished
    {
        get
        {
            return _retainAsPublished;
        }
    }

    public ReadOnlySpan<uint> SubscriptionIdentifiers
    {
        get
        {
            return _identifierSource.GetSubscriptionIdentifiers(
                _identifierOffset,
                _identifierCount);
        }
    }

    internal ReadOnlyMemory<uint> SubscriptionIdentifiersMemory
    {
        get
        {
            return _identifierSource.GetSubscriptionIdentifiersMemory(
                _identifierOffset,
                _identifierCount);
        }
    }

    internal void IncrementIdentifierCount()
    {
        _identifierCount++;
    }

    internal int PrepareIdentifierOffset(int currentOffset)
    {
        _identifierOffset = currentOffset;
        _identifierWriteCount = 0;
        return currentOffset + _identifierCount;
    }

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

    internal int TakeIdentifierWriteIndex()
    {
        int index = _identifierOffset + _identifierWriteCount;
        _identifierWriteCount++;
        return index;
    }
}
