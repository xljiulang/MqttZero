namespace MqttZero.Server.Broker;

internal sealed class MqttZeroFanoutDiagnostics
{
    public const string AppContextSwitchName =
        "MqttZero.FanoutDiagnostics";

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
    private long _fanoutSchedulerBatchCount;
    private long _fanoutSchedulerEnqueueCount;
    private long _fanoutSchedulerFailureCount;
    private long _fanoutFailureCount;
    private long _fanoutSchedulerPassCount;
    private long _maxFanoutSchedulerWaitTicks;
    private long _totalFanoutTicks;
    private long _totalEgressSchedulerTicks;

    public MqttZeroFanoutDiagnostics()
        : this(AppContext.TryGetSwitch(
            AppContextSwitchName,
            out bool enabled) && enabled)
    {
    }

    internal MqttZeroFanoutDiagnostics(bool enabled)
    {
        _enabled = enabled;
    }

    public bool Enabled
    {
        get
        {
            return _enabled;
        }
    }

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
            Interlocked.Read(ref _fanoutFailureCount));
    }

    public void RecordEgressDrain()
    {
        if (_enabled)
        {
            Interlocked.Increment(ref _egressDrainCount);
        }
    }

    public void RecordEgressDrainTiming(long elapsedTicks)
    {
        if (!_enabled)
        {
            return;
        }

        Interlocked.Add(ref _totalEgressDrainTicks, elapsedTicks);
        InterlockedMax(ref _maxEgressDrainTicks, elapsedTicks);
    }

    public void RecordEgressScheduledActor()
    {
        if (_enabled)
        {
            Interlocked.Increment(ref _scheduledActorCount);
        }
    }

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

    public void RecordEgressFlushStart()
    {
        if (_enabled)
        {
            Interlocked.Increment(ref _egressFlushStartCount);
        }
    }

    public void RecordEgressSignal()
    {
        if (_enabled)
        {
            Interlocked.Increment(ref _egressSignalCount);
        }
    }

    public void RecordEncodedPublishes(int count)
    {
        if (_enabled && count > 0)
        {
            Interlocked.Add(ref _encodedPublishCount, count);
        }
    }

    public void RecordFlushWait()
    {
        if (_enabled)
        {
            Interlocked.Increment(ref _flushWaitCount);
        }
    }

    public void RecordFanoutSchedulerEnqueue(long elapsedTicks)
    {
        if (!_enabled)
        {
            return;
        }

        Interlocked.Increment(ref _fanoutSchedulerEnqueueCount);
        InterlockedMax(ref _maxFanoutSchedulerWaitTicks, elapsedTicks);
    }

    public void RecordFanoutSchedulerPass(int batchCount)
    {
        if (!_enabled)
        {
            return;
        }

        Interlocked.Increment(ref _fanoutSchedulerPassCount);
        Interlocked.Add(ref _fanoutSchedulerBatchCount, batchCount);
    }

    public void RecordFanoutSchedulerFailure()
    {
        if (_enabled)
        {
            Interlocked.Increment(ref _fanoutSchedulerFailureCount);
        }
    }

    public void RecordFanoutFailure()
    {
        if (_enabled)
        {
            Interlocked.Increment(ref _fanoutFailureCount);
        }
    }

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
        Interlocked.Exchange(ref _fanoutFailureCount, 0);
    }

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
}
