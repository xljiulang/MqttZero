namespace MqttZero.Server.Broker;

/// <summary>
/// 扇出与出站路径的诊断计数。
/// </summary>
internal sealed class MqttZeroFanoutDiagnostics
{
    public const string AppContextSwitchName = "MqttZero.FanoutDiagnostics";

    private readonly bool _enabled;
    private long _deliveryCount;
    private long _egressDrainCount;
    private long _egressSignalCount;
    private long _encodedPublishCount;
    private long _flushWaitCount;
    private long _localBatchCount;
    private int _maxPendingQueueDepth;
    private long _maxFanoutTicks;
    private long _publishCount;
    private long _remoteBatchCount;
    private long _scheduledActorCount;
    private long _scheduledActorPassCount;
    private long _schedulerPassCount;
    private long _totalEgressDrainTicks;
    private long _maxEgressDrainTicks;
    private long _egressDataBatchCount;
    private long _egressBatchMessageCount;
    private long _egressBatchByteCount;
    private long _egressFlushStartCount;
    private long _maxInflightUpdateTicks;
    private long _maxPendingSelectionTicks;
    private long _maxPublishEncodeTicks;
    private long _maxPublishWriteTicks;
    private long _fanoutSchedulerBatchCount;
    private long _fanoutSchedulerEnqueueCount;
    private long _fanoutSchedulerFailureCount;
    private long _fanoutFailureCount;
    private long _fanoutSchedulerPassCount;
    private long _maxFanoutSchedulerWaitTicks;
    private long _totalFanoutTicks;
    private long _totalEgressSchedulerTicks;
    private long _totalInflightUpdateTicks;
    private long _totalPendingSelectionTicks;
    private long _totalPublishEncodeTicks;
    private long _totalPublishWriteTicks;
    private long _shardOwnershipViolationCount;
    private long _ackWorkCount;
    private long _controlBatchCount;
    private long _controlBatchFrameMax;
    private long _controlBatchFrameSum;
    private long _controlFrameCount;
    private long _controlFrameDirectCount;
    private long _controlFrameQueuedCount;
    private long _maxAckWorkQueueTicks;
    private long _maxControlWriteTicks;
    private long _qosZeroDeferredCount;
    private long _qosZeroDirectCount;
    private long _totalAckWorkQueueTicks;
    private long _totalControlWriteTicks;
    private int _currentEgressFlushPendingCount;
    private long _currentEncodedBytesAwaitingFlush;
    private int _maxEgressFlushPendingCount;
    private long _maxEncodedBytesAwaitingFlush;
    private int _maxInflightQueueDepth;

    /// <summary>
    /// 初始化 MqttZeroFanoutDiagnostics 的新实例。
    /// </summary>
    public MqttZeroFanoutDiagnostics()
        : this(AppContext.TryGetSwitch(
            AppContextSwitchName,
            out bool enabled) && enabled)
    {
    }

    /// <summary>
    /// 初始化 MqttZeroFanoutDiagnostics 的新实例。
    /// </summary>
    /// <param name="enabled">对应的 enabled。</param>
    internal MqttZeroFanoutDiagnostics(bool enabled)
    {
        _enabled = enabled;
    }

    /// <summary>
    /// Enabled 的值。
    /// </summary>
    public bool Enabled
    {
        get
        {
            return _enabled;
        }
    }

    /// <summary>
    /// 获取快照。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public MqttZeroFanoutDiagnosticsSnapshot GetSnapshot()
    {
        return new MqttZeroFanoutDiagnosticsSnapshot(
            Interlocked.Read(ref _publishCount),
            Interlocked.Read(ref _localBatchCount),
            Interlocked.Read(ref _remoteBatchCount),
            Interlocked.Read(ref _deliveryCount),
            Interlocked.Read(ref _totalFanoutTicks),
            Interlocked.Read(ref _maxFanoutTicks),
            Interlocked.Read(ref _egressSignalCount),
            Interlocked.Read(ref _egressDrainCount),
            Interlocked.Read(ref _encodedPublishCount),
            Interlocked.Read(ref _flushWaitCount),
            Volatile.Read(ref _maxPendingQueueDepth),
            Interlocked.Read(ref _scheduledActorCount),
            Interlocked.Read(ref _schedulerPassCount),
            Interlocked.Read(ref _scheduledActorPassCount),
            Interlocked.Read(ref _totalEgressSchedulerTicks),
            Interlocked.Read(ref _totalEgressDrainTicks),
            Interlocked.Read(ref _maxEgressDrainTicks),
            Interlocked.Read(ref _egressDataBatchCount),
            Interlocked.Read(ref _egressBatchMessageCount),
            Interlocked.Read(ref _egressBatchByteCount),
            Interlocked.Read(ref _egressFlushStartCount),
            Interlocked.Read(ref _fanoutSchedulerEnqueueCount),
            Interlocked.Read(ref _fanoutSchedulerPassCount),
            Interlocked.Read(ref _fanoutSchedulerBatchCount),
            Interlocked.Read(ref _fanoutSchedulerFailureCount),
            Interlocked.Read(ref _maxFanoutSchedulerWaitTicks) *
                1_000_000d /
                System.Diagnostics.Stopwatch.Frequency,
            Interlocked.Read(ref _totalPendingSelectionTicks),
            Interlocked.Read(ref _maxPendingSelectionTicks),
            Interlocked.Read(ref _totalInflightUpdateTicks),
            Interlocked.Read(ref _maxInflightUpdateTicks),
            Interlocked.Read(ref _totalPublishEncodeTicks),
            Interlocked.Read(ref _maxPublishEncodeTicks),
            Interlocked.Read(ref _totalPublishWriteTicks),
            Interlocked.Read(ref _maxPublishWriteTicks),
            Interlocked.Read(ref _fanoutFailureCount),
            Interlocked.Read(ref _shardOwnershipViolationCount),
            Interlocked.Read(ref _controlFrameCount),
            Interlocked.Read(ref _controlFrameDirectCount),
            Interlocked.Read(ref _controlFrameQueuedCount),
            Interlocked.Read(ref _controlBatchCount),
            Interlocked.Read(ref _controlBatchFrameSum),
            Interlocked.Read(ref _controlBatchFrameMax),
            Interlocked.Read(ref _totalControlWriteTicks),
            Interlocked.Read(ref _maxControlWriteTicks),
            Interlocked.Read(ref _ackWorkCount),
            Interlocked.Read(ref _totalAckWorkQueueTicks),
            Interlocked.Read(ref _maxAckWorkQueueTicks),
            Interlocked.Read(ref _qosZeroDirectCount),
            Interlocked.Read(ref _qosZeroDeferredCount),
            Volatile.Read(ref _currentEgressFlushPendingCount),
            Interlocked.Read(ref _currentEncodedBytesAwaitingFlush),
            Volatile.Read(ref _maxEgressFlushPendingCount),
            Interlocked.Read(ref _maxEncodedBytesAwaitingFlush),
            Volatile.Read(ref _maxPendingQueueDepth),
            Volatile.Read(ref _maxInflightQueueDepth));
    }

    /// <summary>
    /// 记录出站排空。
    /// </summary>
    public void RecordEgressDrain()
    {
        if (_enabled)
        {
            Interlocked.Increment(ref _egressDrainCount);
        }
    }

    /// <summary>
    /// 记录出站排空Timing。
    /// </summary>
    /// <param name="elapsedTicks">elapsed时钟滴答。</param>
    public void RecordEgressDrainTiming(long elapsedTicks)
    {
        if (!_enabled)
        {
            return;
        }

        Interlocked.Add(ref _totalEgressDrainTicks, elapsedTicks);
        InterlockedMax(ref _maxEgressDrainTicks, elapsedTicks);
    }

    /// <summary>
    /// 记录待处理Selection。
    /// </summary>
    /// <param name="elapsedTicks">elapsed时钟滴答。</param>
    public void RecordPendingSelection(long elapsedTicks)
    {
        if (!_enabled)
        {
            return;
        }

        Interlocked.Add(ref _totalPendingSelectionTicks, elapsedTicks);
        InterlockedMax(ref _maxPendingSelectionTicks, elapsedTicks);
    }

    /// <summary>
    /// 记录在途Update。
    /// </summary>
    /// <param name="elapsedTicks">elapsed时钟滴答。</param>
    public void RecordInflightUpdate(long elapsedTicks)
    {
        if (!_enabled)
        {
            return;
        }

        Interlocked.Add(ref _totalInflightUpdateTicks, elapsedTicks);
        InterlockedMax(ref _maxInflightUpdateTicks, elapsedTicks);
    }

    /// <summary>
    /// 记录发布Encode。
    /// </summary>
    /// <param name="elapsedTicks">elapsed时钟滴答。</param>
    public void RecordPublishEncode(long elapsedTicks)
    {
        if (!_enabled)
        {
            return;
        }

        Interlocked.Add(ref _totalPublishEncodeTicks, elapsedTicks);
        InterlockedMax(ref _maxPublishEncodeTicks, elapsedTicks);
    }

    /// <summary>
    /// 记录发布写入。
    /// </summary>
    /// <param name="elapsedTicks">elapsed时钟滴答。</param>
    public void RecordPublishWrite(long elapsedTicks)
    {
        if (!_enabled)
        {
            return;
        }

        Interlocked.Add(ref _totalPublishWriteTicks, elapsedTicks);
        InterlockedMax(ref _maxPublishWriteTicks, elapsedTicks);
    }

    /// <summary>
    /// 记录出站ScheduledActor。
    /// </summary>
    public void RecordEgressScheduledActor()
    {
        if (_enabled)
        {
            Interlocked.Increment(ref _scheduledActorCount);
        }
    }

    /// <summary>
    /// 记录出站调度器轮次。
    /// </summary>
    /// <param name="actorCount">actor数量。</param>
    /// <param name="elapsedTicks">elapsed时钟滴答。</param>
    public void RecordEgressSchedulerPass(
        int actorCount,
        long elapsedTicks)
    {
        if (!_enabled)
        {
            return;
        }

        Interlocked.Increment(ref _schedulerPassCount);
        Interlocked.Add(ref _scheduledActorPassCount, actorCount);
        Interlocked.Add(ref _totalEgressSchedulerTicks, elapsedTicks);
    }

    /// <summary>
    /// 记录出站数据批次。
    /// </summary>
    /// <param name="messageCount">消息数量。</param>
    /// <param name="encodedBytes">encoded字节数。</param>
    public void RecordEgressDataBatch(
        int messageCount,
        long encodedBytes)
    {
        if (!_enabled)
        {
            return;
        }

        Interlocked.Increment(ref _egressDataBatchCount);
        Interlocked.Add(ref _egressBatchMessageCount, messageCount);
        Interlocked.Add(ref _egressBatchByteCount, encodedBytes);
    }

    /// <summary>
    /// 记录出站FlushStart。
    /// </summary>
    public void RecordEgressFlushStart()
    {
        if (_enabled)
        {
            Interlocked.Increment(ref _egressFlushStartCount);
        }
    }

    /// <summary>
    /// 记录出站Signal。
    /// </summary>
    public void RecordEgressSignal()
    {
        if (_enabled)
        {
            Interlocked.Increment(ref _egressSignalCount);
        }
    }

    /// <summary>
    /// 记录EncodedPublishes。
    /// </summary>
    /// <param name="count">数量。</param>
    public void RecordEncodedPublishes(int count)
    {
        if (_enabled && count > 0)
        {
            Interlocked.Add(ref _encodedPublishCount, count);
        }
    }

    /// <summary>
    /// 记录FlushWait。
    /// </summary>
    public void RecordFlushWait()
    {
        if (_enabled)
        {
            Interlocked.Increment(ref _flushWaitCount);
        }
    }

    /// <summary>
    /// 记录Fanout调度器Enqueue。
    /// </summary>
    /// <param name="elapsedTicks">elapsed时钟滴答。</param>
    public void RecordFanoutSchedulerEnqueue(long elapsedTicks)
    {
        if (!_enabled)
        {
            return;
        }

        Interlocked.Increment(ref _fanoutSchedulerEnqueueCount);
        InterlockedMax(ref _maxFanoutSchedulerWaitTicks, elapsedTicks);
    }

    /// <summary>
    /// 记录Fanout调度器轮次。
    /// </summary>
    /// <param name="batchCount">批次数量。</param>
    public void RecordFanoutSchedulerPass(int batchCount)
    {
        if (!_enabled)
        {
            return;
        }

        Interlocked.Increment(ref _fanoutSchedulerPassCount);
        Interlocked.Add(ref _fanoutSchedulerBatchCount, batchCount);
    }

    /// <summary>
    /// 记录Fanout调度器失败。
    /// </summary>
    public void RecordFanoutSchedulerFailure()
    {
        if (_enabled)
        {
            Interlocked.Increment(ref _fanoutSchedulerFailureCount);
        }
    }

    /// <summary>
    /// 记录Fanout失败。
    /// </summary>
    public void RecordFanoutFailure()
    {
        if (_enabled)
        {
            Interlocked.Increment(ref _fanoutFailureCount);
        }
    }

    /// <summary>
    /// 记录分片归属Violation。
    /// </summary>
    public void RecordShardOwnershipViolation()
    {
        if (_enabled)
        {
            Interlocked.Increment(ref _shardOwnershipViolationCount);
        }
    }

    /// <summary>
    /// 记录控制帧。
    /// </summary>
    /// <param name="direct">对应的 direct。</param>
    public void RecordControlFrame(bool direct)
    {
        if (!_enabled)
        {
            return;
        }

        Interlocked.Increment(ref _controlFrameCount);
        if (direct)
        {
            Interlocked.Increment(ref _controlFrameDirectCount);
        }
        else
        {
            Interlocked.Increment(ref _controlFrameQueuedCount);
        }
    }

    /// <summary>
    /// 记录控制批次。
    /// </summary>
    /// <param name="frameCount">帧数量。</param>
    /// <param name="elapsedTicks">elapsed时钟滴答。</param>
    public void RecordControlBatch(int frameCount, long elapsedTicks)
    {
        if (!_enabled)
        {
            return;
        }

        Interlocked.Increment(ref _controlBatchCount);
        Interlocked.Add(ref _controlBatchFrameSum, frameCount);
        InterlockedMax(ref _controlBatchFrameMax, frameCount);
        Interlocked.Add(ref _totalControlWriteTicks, elapsedTicks);
        InterlockedMax(ref _maxControlWriteTicks, elapsedTicks);
    }

    /// <summary>
    /// 记录确认。
    /// </summary>
    public void RecordAcknowledgement()
    {
        if (_enabled)
        {
            Interlocked.Increment(ref _ackWorkCount);
        }
    }

    /// <summary>
    /// 记录确认队列。
    /// </summary>
    /// <param name="queuedTimestampTicks">queued时间戳时钟滴答。</param>
    public void RecordAcknowledgementQueue(long queuedTimestampTicks)
    {
        if (!_enabled)
        {
            return;
        }

        long elapsed = System.Diagnostics.Stopwatch.GetTimestamp() -
            queuedTimestampTicks;
        if (elapsed < 0)
        {
            elapsed = 0;
        }

        Interlocked.Add(ref _totalAckWorkQueueTicks, elapsed);
        InterlockedMax(ref _maxAckWorkQueueTicks, elapsed);
    }

    /// <summary>
    /// 记录QosZeroRoute。
    /// </summary>
    /// <param name="direct">对应的 direct。</param>
    public void RecordQosZeroRoute(bool direct)
    {
        if (!_enabled)
        {
            return;
        }

        if (direct)
        {
            Interlocked.Increment(ref _qosZeroDirectCount);
        }
        else
        {
            Interlocked.Increment(ref _qosZeroDeferredCount);
        }
    }

    /// <summary>
    /// 记录正在等待 Flush 的编码字节。
    /// </summary>
    /// <param name="encodedBytes">编码字节数。</param>
    internal void RecordEgressFlushStarted(long encodedBytes)
    {
        if (!_enabled)
        {
            return;
        }

        int pendingCount = Interlocked.Increment(
            ref _currentEgressFlushPendingCount);
        long pendingBytes = Interlocked.Add(
            ref _currentEncodedBytesAwaitingFlush,
            encodedBytes);
        InterlockedMax(ref _maxEgressFlushPendingCount, pendingCount);
        InterlockedMax(ref _maxEncodedBytesAwaitingFlush, pendingBytes);
    }

    /// <summary>
    /// 记录一个 Flush 已完成。
    /// </summary>
    /// <param name="encodedBytes">编码字节数。</param>
    internal void RecordEgressFlushCompleted(long encodedBytes)
    {
        if (!_enabled)
        {
            return;
        }

        Interlocked.Decrement(ref _currentEgressFlushPendingCount);
        Interlocked.Add(
            ref _currentEncodedBytesAwaitingFlush,
            -encodedBytes);
    }

    /// <summary>
    /// 记录观测到的最大 Inflight 队列深度。
    /// </summary>
    /// <param name="depth">队列深度。</param>
    internal void RecordInflightQueueDepth(int depth)
    {
        if (_enabled)
        {
            InterlockedMax(ref _maxInflightQueueDepth, depth);
        }
    }

    /// <summary>
    /// 记录待处理队列Depth。
    /// </summary>
    /// <param name="depth">对应的 depth。</param>
    public void RecordPendingQueueDepth(int depth)
    {
        if (!_enabled)
        {
            return;
        }

        int current = Volatile.Read(ref _maxPendingQueueDepth);
        while (depth > current)
        {
            int observed = Interlocked.CompareExchange(
                ref _maxPendingQueueDepth,
                depth,
                current);
            if (observed == current)
            {
                return;
            }

            current = observed;
        }
    }

    /// <summary>
    /// 重置。
    /// </summary>
    public void Reset()
    {
        if (!_enabled)
        {
            return;
        }

        Interlocked.Exchange(ref _publishCount, 0);
        Interlocked.Exchange(ref _localBatchCount, 0);
        Interlocked.Exchange(ref _remoteBatchCount, 0);
        Interlocked.Exchange(ref _deliveryCount, 0);
        Interlocked.Exchange(ref _totalFanoutTicks, 0);
        Interlocked.Exchange(ref _maxFanoutTicks, 0);
        Interlocked.Exchange(ref _egressSignalCount, 0);
        Interlocked.Exchange(ref _egressDrainCount, 0);
        Interlocked.Exchange(ref _encodedPublishCount, 0);
        Interlocked.Exchange(ref _flushWaitCount, 0);
        Interlocked.Exchange(ref _maxPendingQueueDepth, 0);
        Interlocked.Exchange(ref _scheduledActorCount, 0);
        Interlocked.Exchange(ref _schedulerPassCount, 0);
        Interlocked.Exchange(ref _scheduledActorPassCount, 0);
        Interlocked.Exchange(ref _totalEgressSchedulerTicks, 0);
        Interlocked.Exchange(ref _totalEgressDrainTicks, 0);
        Interlocked.Exchange(ref _maxEgressDrainTicks, 0);
        Interlocked.Exchange(ref _egressDataBatchCount, 0);
        Interlocked.Exchange(ref _egressBatchMessageCount, 0);
        Interlocked.Exchange(ref _egressBatchByteCount, 0);
        Interlocked.Exchange(ref _egressFlushStartCount, 0);
        Interlocked.Exchange(ref _fanoutSchedulerEnqueueCount, 0);
        Interlocked.Exchange(ref _fanoutSchedulerPassCount, 0);
        Interlocked.Exchange(ref _fanoutSchedulerBatchCount, 0);
        Interlocked.Exchange(ref _fanoutSchedulerFailureCount, 0);
        Interlocked.Exchange(ref _maxFanoutSchedulerWaitTicks, 0);
        Interlocked.Exchange(ref _totalPendingSelectionTicks, 0);
        Interlocked.Exchange(ref _maxPendingSelectionTicks, 0);
        Interlocked.Exchange(ref _totalInflightUpdateTicks, 0);
        Interlocked.Exchange(ref _maxInflightUpdateTicks, 0);
        Interlocked.Exchange(ref _totalPublishEncodeTicks, 0);
        Interlocked.Exchange(ref _maxPublishEncodeTicks, 0);
        Interlocked.Exchange(ref _totalPublishWriteTicks, 0);
        Interlocked.Exchange(ref _maxPublishWriteTicks, 0);
        Interlocked.Exchange(ref _fanoutFailureCount, 0);
        Interlocked.Exchange(ref _shardOwnershipViolationCount, 0);
        Interlocked.Exchange(ref _controlFrameCount, 0);
        Interlocked.Exchange(ref _controlFrameDirectCount, 0);
        Interlocked.Exchange(ref _controlFrameQueuedCount, 0);
        Interlocked.Exchange(ref _controlBatchCount, 0);
        Interlocked.Exchange(ref _controlBatchFrameSum, 0);
        Interlocked.Exchange(ref _controlBatchFrameMax, 0);
        Interlocked.Exchange(ref _totalControlWriteTicks, 0);
        Interlocked.Exchange(ref _maxControlWriteTicks, 0);
        Interlocked.Exchange(ref _ackWorkCount, 0);
        Interlocked.Exchange(ref _totalAckWorkQueueTicks, 0);
        Interlocked.Exchange(ref _maxAckWorkQueueTicks, 0);
        Interlocked.Exchange(ref _qosZeroDirectCount, 0);
        Interlocked.Exchange(ref _qosZeroDeferredCount, 0);
        Interlocked.Exchange(ref _currentEgressFlushPendingCount, 0);
        Interlocked.Exchange(ref _currentEncodedBytesAwaitingFlush, 0);
        Interlocked.Exchange(ref _maxEgressFlushPendingCount, 0);
        Interlocked.Exchange(ref _maxEncodedBytesAwaitingFlush, 0);
        Interlocked.Exchange(ref _maxInflightQueueDepth, 0);
    }

    /// <summary>
    /// 记录发布。
    /// </summary>
    /// <param name="localBatchCount">本地批次数量。</param>
    /// <param name="remoteBatchCount">远端批次数量。</param>
    /// <param name="deliveryCount">delivery数量。</param>
    /// <param name="elapsedTimestampTicks">elapsed时间戳时钟滴答。</param>
    public void RecordPublish(
        int localBatchCount,
        int remoteBatchCount,
        int deliveryCount,
        long elapsedTimestampTicks)
    {
        if (!_enabled)
        {
            return;
        }

        Interlocked.Increment(ref _publishCount);
        Interlocked.Add(ref _localBatchCount, localBatchCount);
        Interlocked.Add(ref _remoteBatchCount, remoteBatchCount);
        Interlocked.Add(ref _deliveryCount, deliveryCount);
        Interlocked.Add(ref _totalFanoutTicks, elapsedTimestampTicks);
        long current = Volatile.Read(ref _maxFanoutTicks);
        while (elapsedTimestampTicks > current)
        {
            long observed = Interlocked.CompareExchange(
                ref _maxFanoutTicks,
                elapsedTimestampTicks,
                current);
            if (observed == current)
            {
                return;
            }

            current = observed;
        }
    }

    /// <summary>
    /// 执行 InterlockedMax 对应的处理。
    /// </summary>
    /// <param name="target">目标。</param>
    /// <param name="value">值。</param>
    private static void InterlockedMax(ref long target, long value)
    {
        long current = Volatile.Read(ref target);
        while (value > current)
        {
            long observed = Interlocked.CompareExchange(
                ref target,
                value,
                current);
            if (observed == current)
            {
                return;
            }

            current = observed;
        }
    }

    /// <summary>
    /// 执行整数 InterlockedMax。
    /// </summary>
    /// <param name="target">目标。</param>
    /// <param name="value">值。</param>
    private static void InterlockedMax(ref int target, int value)
    {
        int current = Volatile.Read(ref target);
        while (value > current)
        {
            int observed = Interlocked.CompareExchange(
                ref target,
                value,
                current);
            if (observed == current)
            {
                return;
            }

            current = observed;
        }
    }
}
