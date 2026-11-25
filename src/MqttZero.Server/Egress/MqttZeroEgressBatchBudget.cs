namespace MqttZero.Server.Egress;

internal struct MqttZeroEgressBatchBudget
{
    private readonly long _deadlineTimestamp;
    private readonly int _maximumBytes;
    private readonly int _maximumMessages;

    public MqttZeroEgressBatchBudget(
        int maximumBytes,
        int maximumMessages,
        long deadlineTimestamp)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumBytes);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumMessages);
        _maximumBytes = maximumBytes;
        _maximumMessages = maximumMessages;
        _deadlineTimestamp = deadlineTimestamp;
        EncodedBytes = 0;
        MessageCount = 0;
    }

    public long EncodedBytes { get; private set; }

    public int MessageCount { get; private set; }

    public bool TryAdd(int encodedBytes, long timestamp)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(encodedBytes);
        if (MessageCount > 0 &&
            (MessageCount >= _maximumMessages ||
                timestamp >= _deadlineTimestamp ||
                EncodedBytes + encodedBytes > _maximumBytes))
        {
            return false;
        }

        MessageCount++;
        EncodedBytes += encodedBytes;
        return true;
    }
}
