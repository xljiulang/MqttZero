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
    private long _totalFanoutTicks;

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
            Volatile.Read(ref _maxPendingQueueDepth));
    }

    public void RecordEgressDrain()
    {
        if (_enabled)
        {
            Interlocked.Increment(ref _egressDrainCount);
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
}
