using MqttZero.Protocol;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace MqttZero.Server.Routing;

/// <summary>
/// 构建路由计划时使用的可复用缓冲区。
/// </summary>
internal sealed class MqttZeroRoutePlanBuffer : IDisposable
{
    private const int InitialAssignmentCapacity = 16;
    private const int InitialSharedGroupCapacity = 16;

    private readonly ShardSessionTable[] _sessionTables;
    private MqttZeroFanoutBatch[] _batches;
    private IdentifierAssignment[] _identifierAssignments;
    private uint[] _identifiers;
    private MqttZeroSharedRouteEntries[] _sharedGroups;
    private int _identifierAssignmentCount;
    private int _sharedGroupCount;
    private bool _disposed;

    /// <summary>
    /// 初始化 MqttZeroRoutePlanBuffer 的新实例。
    /// </summary>
    /// <param name="shardCount">分片数量。</param>
    public MqttZeroRoutePlanBuffer(int shardCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shardCount);
        ShardCount = shardCount;
        _batches = ArrayPool<MqttZeroFanoutBatch>.Shared.Rent(shardCount);
        _sessionTables = new ShardSessionTable[shardCount];
        for (int shardId = 0; shardId < shardCount; shardId++)
        {
            _batches[shardId] = new MqttZeroFanoutBatch(shardId);
            _sessionTables[shardId] = new ShardSessionTable(
                this,
                shardId);
        }

        _identifierAssignments = ArrayPool<IdentifierAssignment>.Shared.Rent(
            InitialAssignmentCapacity);
        _identifiers = ArrayPool<uint>.Shared.Rent(
            InitialAssignmentCapacity);
        _sharedGroups = ArrayPool<MqttZeroSharedRouteEntries>.Shared.Rent(
            InitialSharedGroupCapacity);
    }

    /// <summary>
    /// 分片数量。
    /// </summary>
    public int ShardCount { get; }

    /// <summary>
    /// 获取保留字节Estimate。
    /// </summary>
    /// <returns>计算得到的数值。</returns>
    internal long GetRetainedByteEstimate()
    {
        long bytes = GetArrayByteCount(_batches) +
            GetArrayByteCount(_identifierAssignments) +
            GetArrayByteCount(_identifiers) +
            GetArrayByteCount(_sharedGroups);
        for (int shardId = 0; shardId < ShardCount; shardId++)
        {
            bytes += _batches[shardId].GetRetainedByteEstimate();
            bytes += _sessionTables[shardId].GetRetainedByteEstimate();
        }

        return bytes;
    }

    /// <summary>
    /// 释放。
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        for (int shardId = 0; shardId < ShardCount; shardId++)
        {
            MqttZeroPlannedDelivery[] deliveries =
                _batches[shardId].DetachDeliveries();
            ArrayPool<MqttZeroPlannedDelivery>.Shared.Return(
                deliveries,
                clearArray: true);
            _sessionTables[shardId].Dispose();
        }

        ArrayPool<MqttZeroFanoutBatch>.Shared.Return(
            _batches,
            clearArray: true);
        ArrayPool<IdentifierAssignment>.Shared.Return(
            _identifierAssignments,
            clearArray: true);
        ArrayPool<uint>.Shared.Return(_identifiers);
        ArrayPool<MqttZeroSharedRouteEntries>.Shared.Return(
            _sharedGroups,
            clearArray: true);
        _batches = [];
        _identifierAssignments = [];
        _identifiers = [];
        _sharedGroups = [];
    }

    /// <summary>
    /// 添加Regular。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="subscription">订阅。</param>
    /// <param name="sender">对应的 sender。</param>
    internal void AddRegular(
        MqttZeroSessionState session,
        MqttZeroSubscription subscription,
        MqttZeroSessionState? sender)
    {
        if (subscription.NoLocal && ReferenceEquals(session, sender))
        {
            return;
        }

        AddCandidate(session, subscription);
    }

    /// <summary>
    /// 添加共享组。
    /// </summary>
    /// <param name="group">组。</param>
    internal void AddSharedGroup(MqttZeroSharedRouteEntries group)
    {
        EnsureSharedGroupCapacity(_sharedGroupCount + 1);
        _sharedGroups[_sharedGroupCount++] = group;
    }

    /// <summary>
    /// Begin计划。
    /// </summary>
    /// <param name="shardCount">分片数量。</param>
    internal void BeginPlan(int shardCount)
    {
        ThrowIfDisposed();
        if (shardCount != ShardCount)
        {
            throw new ArgumentException(
                "The shard count does not match the buffer configuration.",
                nameof(shardCount));
        }

        _identifierAssignmentCount = 0;
        _sharedGroupCount = 0;
        for (int shardId = 0; shardId < ShardCount; shardId++)
        {
            _batches[shardId].Reset();
            _sessionTables[shardId].Reset();
        }
    }

    /// <summary>
    /// 创建计划。
    /// </summary>
    /// <returns>对应的结果。</returns>
    internal MqttZeroRoutePlan CreatePlan()
    {
        ThrowIfDisposed();
        FinalizeIdentifiers();
        int nonEmptyBatchCount = 0;
        int totalDeliveryCount = 0;
        for (int shardId = 0; shardId < ShardCount; shardId++)
        {
            int count = _batches[shardId].Count;
            if (count > 0)
            {
                nonEmptyBatchCount++;
                totalDeliveryCount += count;
            }
        }

        return new MqttZeroRoutePlan(
            _batches,
            ShardCount,
            nonEmptyBatchCount,
            totalDeliveryCount);
    }

    /// <summary>
    /// 获取订阅标识符。
    /// </summary>
    /// <param name="offset">偏移。</param>
    /// <param name="count">数量。</param>
    /// <returns>对应的结果。</returns>
    internal ReadOnlySpan<uint> GetSubscriptionIdentifiers(
        int offset,
        int count)
    {
        return _identifiers.AsSpan(offset, count);
    }

    /// <summary>
    /// 获取订阅标识符内存。
    /// </summary>
    /// <param name="offset">偏移。</param>
    /// <param name="count">数量。</param>
    /// <returns>对应的结果。</returns>
    internal ReadOnlyMemory<uint> GetSubscriptionIdentifiersMemory(
        int offset,
        int count)
    {
        return _identifiers.AsMemory(offset, count);
    }

    /// <summary>
    /// 计划共享组。
    /// </summary>
    /// <param name="sender">对应的 sender。</param>
    internal void PlanSharedGroups(
        MqttZeroSessionState? sender)
    {
        for (int groupIndex = 0;
            groupIndex < _sharedGroupCount;
            groupIndex++)
        {
            MqttZeroSharedRouteEntries group = _sharedGroups[groupIndex];
            int matchingCount = 0;
            foreach (MqttZeroPersistentHashMapEntry<
                MqttZeroSessionState,
                MqttZeroSubscription> entry in group.Entries)
            {
                if (!entry.Value.NoLocal ||
                    !ReferenceEquals(entry.Key, sender))
                {
                    matchingCount++;
                }
            }

            if (matchingCount == 0)
            {
                continue;
            }

            int selectedOffset = group.Cursor.NextOffset(matchingCount);
            foreach (MqttZeroPersistentHashMapEntry<
                MqttZeroSessionState,
                MqttZeroSubscription> entry in group.Entries)
            {
                if (entry.Value.NoLocal &&
                    ReferenceEquals(entry.Key, sender))
                {
                    continue;
                }

                if (selectedOffset > 0)
                {
                    selectedOffset--;
                    continue;
                }

                AddCandidate(entry.Key, entry.Value);
                break;
            }
        }
    }

    /// <summary>
    /// 添加Candidate。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="subscription">订阅。</param>
    private void AddCandidate(
        MqttZeroSessionState session,
        MqttZeroSubscription subscription)
    {
        int shardId = session.ShardId;
        if ((uint)shardId >= (uint)ShardCount)
        {
            throw new InvalidOperationException(
                "A session is assigned to a shard outside the route plan.");
        }

        int deliveryIndex = _sessionTables[shardId].AddOrMerge(
            session,
            subscription.QualityOfServiceLevel,
            subscription.RetainAsPublished);
        if (subscription.SubscriptionIdentifier.HasValue)
        {
            RecordIdentifier(
                shardId,
                deliveryIndex,
                subscription.SubscriptionIdentifier.Value);
        }
    }

    /// <summary>
    /// 确保标识符Assignment容量。
    /// </summary>
    /// <param name="minimumLength">最小长度。</param>
    private void EnsureIdentifierAssignmentCapacity(int minimumLength)
    {
        if (_identifierAssignments.Length >= minimumLength)
        {
            return;
        }

        int capacity = _identifierAssignments.Length * 2;
        while (capacity < minimumLength)
        {
            capacity *= 2;
        }

        IdentifierAssignment[] replacement =
            ArrayPool<IdentifierAssignment>.Shared.Rent(capacity);
        Array.Copy(
            _identifierAssignments,
            replacement,
            _identifierAssignmentCount);
        ArrayPool<IdentifierAssignment>.Shared.Return(
            _identifierAssignments,
            clearArray: true);
        _identifierAssignments = replacement;
    }

    /// <summary>
    /// 确保标识符容量。
    /// </summary>
    /// <param name="minimumLength">最小长度。</param>
    private void EnsureIdentifierCapacity(int minimumLength)
    {
        if (_identifiers.Length >= minimumLength)
        {
            return;
        }

        int capacity = _identifiers.Length * 2;
        while (capacity < minimumLength)
        {
            capacity *= 2;
        }

        uint[] replacement = ArrayPool<uint>.Shared.Rent(capacity);
        ArrayPool<uint>.Shared.Return(_identifiers);
        _identifiers = replacement;
    }

    /// <summary>
    /// 确保共享组容量。
    /// </summary>
    /// <param name="minimumLength">最小长度。</param>
    private void EnsureSharedGroupCapacity(int minimumLength)
    {
        if (_sharedGroups.Length >= minimumLength)
        {
            return;
        }

        int capacity = _sharedGroups.Length * 2;
        while (capacity < minimumLength)
        {
            capacity *= 2;
        }

        MqttZeroSharedRouteEntries[] replacement =
            ArrayPool<MqttZeroSharedRouteEntries>.Shared.Rent(capacity);
        Array.Copy(
            _sharedGroups,
            replacement,
            _sharedGroupCount);
        ArrayPool<MqttZeroSharedRouteEntries>.Shared.Return(
            _sharedGroups,
            clearArray: true);
        _sharedGroups = replacement;
    }

    /// <summary>
    /// Finalize标识符。
    /// </summary>
    private void FinalizeIdentifiers()
    {
        EnsureIdentifierCapacity(_identifierAssignmentCount);
        int nextOffset = 0;
        for (int shardId = 0; shardId < ShardCount; shardId++)
        {
            MqttZeroFanoutBatch batch = _batches[shardId];
            for (int deliveryIndex = 0;
                deliveryIndex < batch.Count;
                deliveryIndex++)
            {
                ref MqttZeroPlannedDelivery delivery =
                    ref batch.GetDeliveryReference(deliveryIndex);
                nextOffset = delivery.PrepareIdentifierOffset(nextOffset);
            }
        }

        if (nextOffset != _identifierAssignmentCount)
        {
            throw new InvalidOperationException(
                "The route plan identifier count is inconsistent.");
        }

        for (int assignmentIndex = 0;
            assignmentIndex < _identifierAssignmentCount;
            assignmentIndex++)
        {
            IdentifierAssignment assignment =
                _identifierAssignments[assignmentIndex];
            ref MqttZeroPlannedDelivery delivery =
                ref _batches[assignment.ShardId]
                    .GetDeliveryReference(assignment.DeliveryIndex);
            int identifierIndex = delivery.TakeIdentifierWriteIndex();
            _identifiers[identifierIndex] = assignment.Value;
        }
    }

    /// <summary>
    /// 记录标识符。
    /// </summary>
    /// <param name="shardId">分片Id。</param>
    /// <param name="deliveryIndex">delivery索引。</param>
    /// <param name="identifier">标识符。</param>
    private void RecordIdentifier(
        int shardId,
        int deliveryIndex,
        uint identifier)
    {
        EnsureIdentifierAssignmentCapacity(
            _identifierAssignmentCount + 1);
        _identifierAssignments[_identifierAssignmentCount++] =
            new IdentifierAssignment(
                shardId,
                deliveryIndex,
                identifier);
        ref MqttZeroPlannedDelivery delivery =
            ref _batches[shardId].GetDeliveryReference(deliveryIndex);
        delivery.IncrementIdentifierCount();
    }

    /// <summary>
    /// 抛出IfDisposed。
    /// </summary>
    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    /// <summary>
    /// 获取Array字节数量。
    /// </summary>
    /// <param name="array">对应的 array。</param>
    /// <returns>计算得到的数值。</returns>
    private static long GetArrayByteCount<T>(T[] array)
    {
        return (long)array.Length * Unsafe.SizeOf<T>();
    }

    /// <summary>
    /// 添加Delivery。
    /// </summary>
    /// <param name="shardId">分片Id。</param>
    /// <param name="session">会话。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="retainAsPublished">保留AsPublished。</param>
    /// <returns>计算得到的数值。</returns>
    private int AddDelivery(
        int shardId,
        MqttZeroSessionState session,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retainAsPublished)
    {
        return _batches[shardId].Add(
            session,
            qualityOfServiceLevel,
            retainAsPublished,
            this);
    }

    /// <summary>
    /// 合并DeliverySelection。
    /// </summary>
    /// <param name="shardId">分片Id。</param>
    /// <param name="deliveryIndex">delivery索引。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="retainAsPublished">保留AsPublished。</param>
    private void MergeDeliverySelection(
        int shardId,
        int deliveryIndex,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retainAsPublished)
    {
        ref MqttZeroPlannedDelivery delivery =
            ref _batches[shardId].GetDeliveryReference(deliveryIndex);
        delivery.MergeSelection(
            qualityOfServiceLevel,
            retainAsPublished);
    }

    /// <summary>
    /// 订阅标识符的分配结果。
    /// </summary>
    private readonly struct IdentifierAssignment
    {
        /// <summary>
        /// 初始化 IdentifierAssignment 的新实例。
        /// </summary>
        /// <param name="shardId">分片Id。</param>
        /// <param name="deliveryIndex">delivery索引。</param>
        /// <param name="value">值。</param>
        public IdentifierAssignment(
            int shardId,
            int deliveryIndex,
            uint value)
        {
            ShardId = shardId;
            DeliveryIndex = deliveryIndex;
            Value = value;
        }

        /// <summary>
        /// 分片Id。
        /// </summary>
        public int ShardId { get; }

        /// <summary>
        /// Delivery索引。
        /// </summary>
        public int DeliveryIndex { get; }

        /// <summary>
        /// 值。
        /// </summary>
        public uint Value { get; }
    }

    /// <summary>
    /// 按分片分组的会话表。
    /// </summary>
    private sealed class ShardSessionTable : IDisposable
    {
        private const int InitialCapacity = 16;

        private readonly MqttZeroRoutePlanBuffer _owner;
        private readonly int _shardId;
        private int _capacity;
        private int[] _deliveryIndexes;
        private int[] _generations;
        private MqttZeroSessionState?[] _sessions;
        private int _count;
        private int _generation;

        /// <summary>
        /// 初始化 ShardSessionTable 的新实例。
        /// </summary>
        /// <param name="owner">持有者。</param>
        /// <param name="shardId">分片Id。</param>
        public ShardSessionTable(
            MqttZeroRoutePlanBuffer owner,
            int shardId)
        {
            _owner = owner;
            _shardId = shardId;
            _capacity = InitialCapacity;
            _sessions = ArrayPool<MqttZeroSessionState?>.Shared.Rent(
                _capacity);
            _deliveryIndexes = ArrayPool<int>.Shared.Rent(_capacity);
            _generations = ArrayPool<int>.Shared.Rent(_capacity);
            Array.Clear(_sessions);
            Array.Clear(_generations);
        }

        /// <summary>
        /// 添加OrMerge。
        /// </summary>
        /// <param name="session">会话。</param>
        /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
        /// <param name="retainAsPublished">保留AsPublished。</param>
        /// <returns>计算得到的数值。</returns>
        public int AddOrMerge(
            MqttZeroSessionState session,
            MqttQualityOfServiceLevel qualityOfServiceLevel,
            bool retainAsPublished)
        {
            int slot = FindSlot(session, out int deliveryIndex);
            if (deliveryIndex >= 0)
            {
                _owner.MergeDeliverySelection(
                    _shardId,
                    deliveryIndex,
                    qualityOfServiceLevel,
                    retainAsPublished);
                return deliveryIndex;
            }

            if ((_count + 1) * 2 >= _capacity)
            {
                Grow();
                slot = FindSlot(session, out deliveryIndex);
            }

            deliveryIndex = _owner.AddDelivery(
                _shardId,
                session,
                qualityOfServiceLevel,
                retainAsPublished);
            _sessions[slot] = session;
            _deliveryIndexes[slot] = deliveryIndex;
            _generations[slot] = _generation;
            _count++;
            return deliveryIndex;
        }

        /// <summary>
        /// 释放。
        /// </summary>
        public void Dispose()
        {
            ArrayPool<MqttZeroSessionState?>.Shared.Return(
                _sessions,
                clearArray: true);
            ArrayPool<int>.Shared.Return(_deliveryIndexes);
            ArrayPool<int>.Shared.Return(_generations);
            _sessions = [];
            _deliveryIndexes = [];
            _generations = [];
        }

        /// <summary>
        /// 获取保留字节Estimate。
        /// </summary>
        /// <returns>计算得到的数值。</returns>
        internal long GetRetainedByteEstimate()
        {
            return
                (long)_sessions.Length *
                    Unsafe.SizeOf<MqttZeroSessionState?>() +
                (long)_deliveryIndexes.Length * sizeof(int) +
                (long)_generations.Length * sizeof(int);
        }

        /// <summary>
        /// 重置。
        /// </summary>
        public void Reset()
        {
            _count = 0;
            _generation++;
            if (_generation == 0)
            {
                Array.Clear(_generations);
                _generation = 1;
            }
        }

        /// <summary>
        /// 查找Slot。
        /// </summary>
        /// <param name="session">会话。</param>
        /// <param name="deliveryIndex">delivery索引。</param>
        /// <returns>计算得到的数值。</returns>
        private int FindSlot(
            MqttZeroSessionState session,
            out int deliveryIndex)
        {
            int mask = _capacity - 1;
            int slot = RuntimeHelpers.GetHashCode(session) & mask;
            while (true)
            {
                MqttZeroSessionState? current = _sessions[slot];
                if (_generations[slot] != _generation ||
                    current == null)
                {
                    deliveryIndex = -1;
                    return slot;
                }

                if (ReferenceEquals(current, session))
                {
                    deliveryIndex = _deliveryIndexes[slot];
                    return slot;
                }

                slot = (slot + 1) & mask;
            }
        }

        /// <summary>
        /// 执行 Grow 对应的处理。
        /// </summary>
        private void Grow()
        {
            int capacity = _capacity * 2;
            MqttZeroSessionState?[] sessions =
                ArrayPool<MqttZeroSessionState?>.Shared.Rent(capacity);
            int[] deliveryIndexes =
                ArrayPool<int>.Shared.Rent(capacity);
            int[] generations = ArrayPool<int>.Shared.Rent(capacity);
            Array.Clear(sessions);
            Array.Clear(generations);
            int mask = capacity - 1;
            for (int source = 0; source < _capacity; source++)
            {
                MqttZeroSessionState? session = _sessions[source];
                if (_generations[source] != _generation ||
                    session == null)
                {
                    continue;
                }

                int slot = RuntimeHelpers.GetHashCode(session) & mask;
                while (sessions[slot] != null &&
                    generations[slot] == _generation)
                {
                    slot = (slot + 1) & mask;
                }

                sessions[slot] = session;
                deliveryIndexes[slot] = _deliveryIndexes[source];
                generations[slot] = _generation;
            }

            ArrayPool<MqttZeroSessionState?>.Shared.Return(
                _sessions,
                clearArray: true);
            ArrayPool<int>.Shared.Return(_deliveryIndexes);
            ArrayPool<int>.Shared.Return(_generations);
            _sessions = sessions;
            _deliveryIndexes = deliveryIndexes;
            _generations = generations;
            _capacity = capacity;
        }
    }
}
