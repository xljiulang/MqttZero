namespace MqttZero.Server.Egress;

/// <summary>
/// 单次出站数据批次的字节、条数与时间预算。
/// </summary>
internal struct MqttZeroEgressBatchBudget
{
    private readonly long _deadlineTimestamp;
    private readonly int _maximumBytes;
    private readonly int _maximumMessages;

    /// <summary>
    /// 初始化 MqttZeroEgressBatchBudget 的新实例。
    /// </summary>
    /// <param name="maximumBytes">最大字节数。</param>
    /// <param name="maximumMessages">最大消息。</param>
    /// <param name="deadlineTimestamp">截止时间时间戳。</param>
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

    /// <summary>
    /// Encoded字节数。
    /// </summary>
    public long EncodedBytes { get; private set; }

    /// <summary>
    /// 消息数量。
    /// </summary>
    public int MessageCount { get; private set; }

    /// <summary>
    /// 尝试Add。
    /// </summary>
    /// <param name="encodedBytes">encoded字节数。</param>
    /// <param name="timestamp">时间戳。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
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
