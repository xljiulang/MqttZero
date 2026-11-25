using MqttZero.Protocol;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace MqttZero.Server.Routing;

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

    public int ShardCount { get; }

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

    internal void AddSharedGroup(MqttZeroSharedRouteEntries group)
    {
        EnsureSharedGroupCapacity(_sharedGroupCount + 1);
        _sharedGroups[_sharedGroupCount++] = group;
    }

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

    internal ReadOnlySpan<uint> GetSubscriptionIdentifiers(
        int offset,
        int count)
    {
        return _identifiers.AsSpan(offset, count);
    }

    internal ReadOnlyMemory<uint> GetSubscriptionIdentifiersMemory(
        int offset,
        int count)
    {
        return _identifiers.AsMemory(offset, count);
    }

    internal void PlanSharedGroups(
        MqttZeroSessionState? sender,
        MqttZeroSharedGroupCursors cursors)
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

            int selectedOffset = cursors.GetNext(
                group.Key,
                matchingCount);
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

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

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

    private readonly struct IdentifierAssignment
    {
        public IdentifierAssignment(
            int shardId,
            int deliveryIndex,
            uint value)
        {
            ShardId = shardId;
            DeliveryIndex = deliveryIndex;
            Value = value;
        }

        public int ShardId { get; }

        public int DeliveryIndex { get; }

        public uint Value { get; }
    }

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
