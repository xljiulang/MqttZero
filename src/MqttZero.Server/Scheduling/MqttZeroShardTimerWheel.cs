using MqttZero.Server.Broker;
using System.Diagnostics;

namespace MqttZero.Server.Scheduling;

internal sealed class MqttZeroShardTimerWheel : IAsyncDisposable
{
    private const int LevelBits = 9;
    private const int Levels = 4;
    private const int SlotsPerLevel = 1 << LevelBits;
    private const long LevelMask = SlotsPerLevel - 1;

    private readonly MqttZeroTimerNode?[,] _buckets =
        new MqttZeroTimerNode?[Levels, SlotsPerLevel];
    private readonly List<MqttZeroTimerNode> _cascadeNodes = [];
    private readonly Func<
        MqttZeroTimerNode,
        CancellationToken,
        ValueTask> _callback;
    private readonly MqttZeroFanoutDiagnostics? _diagnostics;
    private readonly List<MqttZeroTimerNode> _dueNodes = [];
    private readonly MqttZeroEventLoop _shard;
    private readonly TaskCompletionSource _stopped = new(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _tickInterval;
    private readonly long _tickIntervalUtcTicks;
    private readonly MqttZeroTimerTickWork _tickWork;
    private CancellationTokenSource? _tickerCancellation;
    private Task? _tickerTask;
    private long _currentTick;
    private long _nextSequence;
    private int _started;
    private int _stopping;
    private int _tickPending;
    private int _tickScheduled;
    private int _count;
    private long _oldestDueUtcTicks = long.MaxValue;

    internal MqttZeroShardTimerWheel(
        MqttZeroEventLoop shard,
        TimeProvider timeProvider,
        TimeSpan tickInterval,
        Func<
            MqttZeroTimerNode,
            CancellationToken,
            ValueTask> callback,
        MqttZeroFanoutDiagnostics? diagnostics)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(
            tickInterval,
            TimeSpan.Zero);
        _shard = shard ?? throw new ArgumentNullException(nameof(shard));
        _timeProvider = timeProvider ??
            throw new ArgumentNullException(nameof(timeProvider));
        _tickInterval = tickInterval;
        _tickIntervalUtcTicks = Math.Max(1, tickInterval.Ticks);
        _callback = callback ??
            throw new ArgumentNullException(nameof(callback));
        _diagnostics = diagnostics;
        _currentTick = FloorTick(timeProvider.GetUtcNow().UtcTicks);
        _tickWork = new MqttZeroTimerTickWork(this);
    }

    internal int Count
    {
        get
        {
            return Volatile.Read(ref _count);
        }
    }

    internal int ShardId
    {
        get
        {
            return _shard.ShardId;
        }
    }

    internal void Start()
    {
        if (Interlocked.Exchange(ref _started, 1) != 0)
        {
            return;
        }

        _tickerCancellation = new CancellationTokenSource();
        _tickerTask = RunTickerAsync(_tickerCancellation.Token);
    }

    internal void ScheduleOrRemove(
        MqttZeroTimerNode node,
        long dueUtcTicks)
    {
        ArgumentNullException.ThrowIfNull(node);
        if (Volatile.Read(ref _stopping) != 0)
        {
            return;
        }

        if (node.Wheel != null &&
            !ReferenceEquals(node.Wheel, this))
        {
            node.Wheel.Cancel(node);
        }

        if (node.IsScheduled)
        {
            RemoveFromBucket(node);
        }

        node.DueUtcTicks = dueUtcTicks;
        node.Version++;
        node.Sequence = Interlocked.Increment(ref _nextSequence);
        node.Wheel = this;
        AddToBucket(
            node,
            GetLevel(dueUtcTicks),
            GetSlot(dueUtcTicks, GetLevel(dueUtcTicks)));
        UpdateOldestDue(dueUtcTicks);
    }

    internal void Cancel(MqttZeroTimerNode node)
    {
        ArgumentNullException.ThrowIfNull(node);
        if (!ReferenceEquals(node.Wheel, this))
        {
            return;
        }

        if (node.IsScheduled)
        {
            RemoveFromBucket(node);
        }

        node.Version++;
        node.Wheel = null;
    }

    internal async ValueTask TickAsync(
        long nowUtcTicks,
        CancellationToken cancellationToken)
    {
        MqttZeroShardOwnership.EnsureCurrent(
            _shard.ShardId,
            nameof(TickAsync),
            _diagnostics);
        long targetTick = FloorTick(nowUtcTicks);
        if (targetTick > _currentTick)
        {
            await DrainLevelZeroAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        while (_currentTick < targetTick)
        {
            _currentTick++;
            CascadeIfNeeded(3);
            CascadeIfNeeded(2);
            CascadeIfNeeded(1);
            await DrainLevelZeroAsync(
                cancellationToken).ConfigureAwait(false);
        }

        if (_currentTick == targetTick)
        {
            await DrainLevelZeroAsync(cancellationToken)
                .ConfigureAwait(false);
        }
    }

    internal MqttZeroShardTimerSnapshot GetSnapshot()
    {
        int[] levelCounts = new int[Levels];
        DateTimeOffset now = _timeProvider.GetUtcNow();
        for (int level = 0; level < Levels; level++)
        {
            for (int slot = 0; slot < SlotsPerLevel; slot++)
            {
                for (MqttZeroTimerNode? node = _buckets[level, slot];
                    node != null;
                    node = node.BucketNext)
                {
                    levelCounts[level]++;
                }
            }
        }

        double oldestAgeMilliseconds =
            _oldestDueUtcTicks == long.MaxValue
                ? 0
                : Math.Max(
                    0,
                    (now.UtcTicks - _oldestDueUtcTicks) *
                        1000d /
                        TimeSpan.TicksPerMillisecond);
        return new MqttZeroShardTimerSnapshot(
            _shard.ShardId,
            Count,
            _currentTick,
            levelCounts,
            oldestAgeMilliseconds);
    }

    internal async ValueTask StopAsync()
    {
        if (Interlocked.Exchange(ref _stopping, 1) == 0)
        {
            CancellationTokenSource? cancellation = _tickerCancellation;
            _tickerCancellation = null;
            if (cancellation != null)
            {
                await cancellation.CancelAsync().ConfigureAwait(false);
            }

            if (_tickerTask != null)
            {
                try
                {
                    await _tickerTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
            }

            cancellation?.Dispose();
            _tickerTask = null;
            Clear();
            TryCompleteStop();
        }

        await _stopped.Task.ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
    }

    internal async ValueTask ExecuteTickWorkAsync(
        CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _stopping) != 0)
        {
            return;
        }

        Volatile.Write(ref _tickPending, 0);
        await TickAsync(
            _timeProvider.GetUtcNow().UtcTicks,
            cancellationToken).ConfigureAwait(false);
    }

    internal void OnTickWorkReleased()
    {
        Volatile.Write(ref _tickScheduled, 0);
        Thread.MemoryBarrier();
        if (Volatile.Read(ref _stopping) != 0)
        {
            TryCompleteStop();
            return;
        }

        if (Volatile.Read(ref _tickPending) != 0)
        {
            ScheduleTickWork();
        }
    }

    private async Task RunTickerAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(
                    _tickInterval,
                    _timeProvider,
                    cancellationToken).ConfigureAwait(false);
                SignalTick();
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private void SignalTick()
    {
        if (Volatile.Read(ref _stopping) != 0)
        {
            TryCompleteStop();
            return;
        }

        Volatile.Write(ref _tickPending, 1);
        ScheduleTickWork();
    }

    private void ScheduleTickWork()
    {
        if (Volatile.Read(ref _stopping) != 0 ||
            Interlocked.CompareExchange(ref _tickScheduled, 1, 0) != 0)
        {
            return;
        }

        if (!_shard.TryEnqueue(_tickWork))
        {
            _ = EnqueueTickWorkAsync();
        }
    }

    private async Task EnqueueTickWorkAsync()
    {
        try
        {
            await _shard.EnqueueAsync(
                _tickWork,
                CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception)
        {
            Volatile.Write(ref _tickScheduled, 0);
            Volatile.Write(ref _stopping, 1);
            TryCompleteStop();
        }
    }

    private void CascadeIfNeeded(int level)
    {
        long span = 1L << (LevelBits * level);
        if (_currentTick % span != 0)
        {
            return;
        }

        int slot = GetSlotForTick(_currentTick, level);
        MqttZeroTimerNode? node = _buckets[level, slot];
        if (node == null)
        {
            return;
        }

        DetachBucket(node, level, slot);
        while (node != null)
        {
            MqttZeroTimerNode? next = node.BucketNext;
            node.BucketNext = null;
            node.BucketPrevious = null;
            node.Level = -1;
            node.Slot = -1;
            _cascadeNodes.Add(node);
            node = next;
        }

        for (int index = 0; index < _cascadeNodes.Count; index++)
        {
            MqttZeroTimerNode candidate = _cascadeNodes[index];
            int targetLevel = GetLevel(candidate.DueUtcTicks);
            int targetSlot = GetSlot(
                candidate.DueUtcTicks,
                targetLevel);
            AddToBucket(
                candidate,
                targetLevel,
                targetSlot,
                incrementCount: false);
        }

        _cascadeNodes.Clear();
    }

    private async ValueTask DrainLevelZeroAsync(
        CancellationToken cancellationToken)
    {
        int slot = GetSlotForTick(_currentTick, 0);
        MqttZeroTimerNode? node = _buckets[0, slot];
        if (node == null)
        {
            return;
        }

        DetachBucket(node, 0, slot);
        while (node != null)
        {
            MqttZeroTimerNode? next = node.BucketNext;
            node.BucketNext = null;
            node.BucketPrevious = null;
            node.Level = -1;
            node.Slot = -1;
            DecrementCount();
            node.Version++;
            _dueNodes.Add(node);
            node = next;
        }

        _dueNodes.Sort(static (left, right) =>
        {
            int dueComparison = left.DueUtcTicks.CompareTo(
                right.DueUtcTicks);
            return dueComparison != 0
                ? dueComparison
                : left.Sequence.CompareTo(right.Sequence);
        });
        try
        {
            for (int index = 0; index < _dueNodes.Count; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                MqttZeroTimerNode due = _dueNodes[index];
                due.Wheel = null;
                await _callback(due, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        finally
        {
            _dueNodes.Clear();
        }
    }

    private int GetLevel(long dueUtcTicks)
    {
        long dueTick = FloorTick(dueUtcTicks);
        long delta = Math.Max(0, dueTick - _currentTick);
        if (delta < SlotsPerLevel)
        {
            return 0;
        }

        long level1Span = (long)SlotsPerLevel * SlotsPerLevel;
        if (delta < level1Span)
        {
            return 1;
        }

        long level2Span = level1Span * SlotsPerLevel;
        if (delta < level2Span)
        {
            return 2;
        }

        return 3;
    }

    private int GetSlot(long dueUtcTicks, int level)
    {
        long dueTick = FloorTick(dueUtcTicks);
        if (level == 0 && dueTick <= _currentTick)
        {
            return GetSlotForTick(_currentTick, 0);
        }

        return GetSlotForTick(dueTick, level);
    }

    private static int GetSlotForTick(long tick, int level)
    {
        return (int)((tick >> (LevelBits * level)) & LevelMask);
    }

    private long FloorTick(long utcTicks)
    {
        return utcTicks / _tickIntervalUtcTicks;
    }

    private void AddToBucket(
        MqttZeroTimerNode node,
        int level,
        int slot,
        bool incrementCount = true)
    {
        node.Level = level;
        node.Slot = slot;
        node.BucketPrevious = null;
        node.BucketNext = _buckets[level, slot];
        if (node.BucketNext != null)
        {
            node.BucketNext.BucketPrevious = node;
        }

        _buckets[level, slot] = node;
        if (incrementCount)
        {
            Interlocked.Increment(ref _count);
        }
    }

    private void RemoveFromBucket(MqttZeroTimerNode node)
    {
        int level = node.Level;
        int slot = node.Slot;
        MqttZeroTimerNode? previous = node.BucketPrevious;
        MqttZeroTimerNode? next = node.BucketNext;
        if (previous != null)
        {
            previous.BucketNext = next;
        }
        else if (ReferenceEquals(_buckets[level, slot], node))
        {
            _buckets[level, slot] = next;
        }

        if (next != null)
        {
            next.BucketPrevious = previous;
        }

        node.BucketPrevious = null;
        node.BucketNext = null;
        node.Level = -1;
        node.Slot = -1;
        DecrementCount();
    }

    private void DetachBucket(
        MqttZeroTimerNode head,
        int level,
        int slot)
    {
        if (ReferenceEquals(_buckets[level, slot], head))
        {
            _buckets[level, slot] = null;
        }
    }

    private void DecrementCount()
    {
        int count = Interlocked.Decrement(ref _count);
        if (count == 0)
        {
            Volatile.Write(ref _oldestDueUtcTicks, long.MaxValue);
        }
    }

    private void UpdateOldestDue(long dueUtcTicks)
    {
        long current;
        do
        {
            current = Volatile.Read(ref _oldestDueUtcTicks);
            if (current <= dueUtcTicks)
            {
                return;
            }
        }
        while (Interlocked.CompareExchange(
            ref _oldestDueUtcTicks,
            dueUtcTicks,
            current) != current);
    }

    private void Clear()
    {
        for (int level = 0; level < Levels; level++)
        {
            for (int slot = 0; slot < SlotsPerLevel; slot++)
            {
                MqttZeroTimerNode? node = _buckets[level, slot];
                while (node != null)
                {
                    MqttZeroTimerNode? next = node.BucketNext;
                    node.BucketPrevious = null;
                    node.BucketNext = null;
                    node.Level = -1;
                    node.Slot = -1;
                    node.Wheel = null;
                    node = next;
                }

                _buckets[level, slot] = null;
            }
        }

        Volatile.Write(ref _count, 0);
        Volatile.Write(ref _oldestDueUtcTicks, long.MaxValue);
        _cascadeNodes.Clear();
        _dueNodes.Clear();
    }

    private bool TryCompleteStop()
    {
        if (Volatile.Read(ref _stopping) == 0)
        {
            return false;
        }

        _stopped.TrySetResult();
        return true;
    }
}
