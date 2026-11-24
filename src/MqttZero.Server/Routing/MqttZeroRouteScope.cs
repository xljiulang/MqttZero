using MqttZero.Protocol;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace MqttZero.Server.Routing;

internal sealed class MqttZeroRouteScope : IDisposable
{
    private MqttZeroDeliveryCandidate[] _deliveries = [];
    private int[] _bestDeliveryIndexes = [];
    private int _bestCapacity;
    private int[] _bestEpochs = [];
    private MqttZeroSessionState?[] _bestSessions = [];
    private int _bestCount;
    private int _epoch;
    private int _deliveryCount;
    private int[] _matchedGroupIndexes = [];
    private int _matchedGroupCount;
    private MqttZeroRouteScopePool? _owner;

    public MqttZeroRouteScope()
    {
    }

    public int Count
    {
        get
        {
            return _deliveryCount;
        }
    }

    public MqttZeroDeliveryCandidate this[int index]
    {
        get
        {
            return _deliveries[index];
        }
    }

    public void SetOwner(MqttZeroRouteScopePool owner)
    {
        _owner = owner;
        _epoch++;
        if (_epoch == 0)
        {
            Array.Clear(_bestEpochs);
            _epoch = 1;
        }

        _bestCount = 0;
    }

    public void AddDelivery(MqttZeroDeliveryCandidate candidate)
    {
        EnsureDeliveryCapacity(_deliveryCount + 1);
        _deliveries[_deliveryCount++] = candidate;
    }

    public void AddRegular(
        MqttZeroSessionState session,
        MqttZeroSubscription subscription)
    {
        EnsureBestCapacity();
        int mask = _bestCapacity - 1;
        int slot = RuntimeHelpers.GetHashCode(session) & mask;
        while (true)
        {
            if (_bestEpochs[slot] != _epoch ||
                _bestSessions[slot] == null)
            {
                int deliveryIndex = _deliveryCount;
                AddDelivery(new MqttZeroDeliveryCandidate(
                    session,
                    subscription,
                    ToIdentifiers(subscription)));
                _bestSessions[slot] = session;
                _bestDeliveryIndexes[slot] = deliveryIndex;
                _bestEpochs[slot] = _epoch;
                _bestCount++;
                if (_bestCount * 2 >= _bestSessions.Length)
                {
                    GrowBestTable();
                }

                return;
            }

            if (ReferenceEquals(_bestSessions[slot], session))
            {
                int deliveryIndex = _bestDeliveryIndexes[slot];
                MqttZeroDeliveryCandidate current = _deliveries[deliveryIndex];
                MqttZeroSubscription selected =
                    subscription.QualityOfServiceLevel >
                        current.QualityOfServiceLevel
                        ? subscription
                        : current.Subscription;
                _deliveries[deliveryIndex] = new MqttZeroDeliveryCandidate(
                    session,
                    selected,
                    AppendIdentifier(
                        current.SubscriptionIdentifiers,
                        subscription.SubscriptionIdentifier));

                return;
            }

            slot = (slot + 1) & mask;
        }
    }

    public void AddMatchedGroupIndex(int groupIndex)
    {
        if (_matchedGroupIndexes.Length == 0)
        {
            _matchedGroupIndexes = ArrayPool<int>.Shared.Rent(16);
        }
        else if (_matchedGroupCount == _matchedGroupIndexes.Length)
        {
            int[] replacement = ArrayPool<int>.Shared.Rent(
                _matchedGroupIndexes.Length * 2);
            Array.Copy(
                _matchedGroupIndexes,
                replacement,
                _matchedGroupCount);
            ArrayPool<int>.Shared.Return(_matchedGroupIndexes);
            _matchedGroupIndexes = replacement;
        }

        _matchedGroupIndexes[_matchedGroupCount++] = groupIndex;
    }

    public void AddRegularGroups(
        MqttZeroSubscriptionRouteSnapshot snapshot,
        MqttZeroSessionState? sender)
    {
        if (_matchedGroupCount > 1)
        {
            Array.Sort(
                _matchedGroupIndexes,
                0,
                _matchedGroupCount);
        }

        int previous = -1;
        for (int matchIndex = 0;
            matchIndex < _matchedGroupCount;
            matchIndex++)
        {
            int groupIndex = _matchedGroupIndexes[matchIndex];
            if (groupIndex == previous)
            {
                continue;
            }

            previous = groupIndex;
            MqttZeroRouteFilterGroup group =
                snapshot.RegularGroups[groupIndex];
            for (int entryIndex = 0;
                entryIndex < group.Entries.Length;
                entryIndex++)
            {
                MqttZeroRouteEntry entry = group.Entries[entryIndex];
                if (!entry.Subscription.NoLocal ||
                    !ReferenceEquals(entry.Session, sender))
                {
                    AddRegular(entry.Session, entry.Subscription);
                }
            }
        }

        _matchedGroupCount = 0;
    }

    public void AddSharedGroup(
        MqttZeroSharedRouteGroup group,
        ReadOnlySpan<byte> topic,
        MqttZeroSessionState? sender,
        ConcurrentDictionary<string, int> cursors)
    {
        int matchingSessionCount = 0;
        for (int index = 0; index < group.Entries.Length; index++)
        {
            MqttZeroSharedRouteEntry entry = group.Entries[index];
            if (TryGetBestSubscription(entry, topic, sender, out _, out _))
            {
                matchingSessionCount++;
            }
        }

        if (matchingSessionCount == 0)
        {
            return;
        }

        int cursor = cursors.AddOrUpdate(
            group.Group,
            0,
            static (_, current) => current == int.MaxValue ? 0 : current + 1);
        int selectedOffset = cursor % matchingSessionCount;
        for (int index = 0; index < group.Entries.Length; index++)
        {
            MqttZeroSharedRouteEntry entry = group.Entries[index];
            if (!TryGetBestSubscription(
                entry,
                topic,
                sender,
                out MqttZeroSubscription? subscription,
                out ReadOnlyMemory<uint> subscriptionIdentifiers))
            {
                continue;
            }

            if (selectedOffset == 0)
            {
                AddDelivery(new MqttZeroDeliveryCandidate(
                    entry.Session,
                    subscription,
                    subscriptionIdentifiers));
                return;
            }

            selectedOffset--;
        }
    }

    public void Dispose()
    {
        Array.Clear(_deliveries, 0, _deliveryCount);
        _deliveryCount = 0;
        _matchedGroupCount = 0;
        _bestCount = 0;
        MqttZeroRouteScopePool owner = _owner ??
            throw new InvalidOperationException("The route scope has no owner pool.");
        _owner = null;
        owner.Return(this);
    }

    public void ReleaseBuffers()
    {
        if (_deliveries.Length > 0)
        {
            ArrayPool<MqttZeroDeliveryCandidate>.Shared.Return(_deliveries, true);
            _deliveries = [];
        }

        ReturnBestArrays();
        if (_matchedGroupIndexes.Length > 0)
        {
            ArrayPool<int>.Shared.Return(_matchedGroupIndexes);
            _matchedGroupIndexes = [];
        }
        _deliveryCount = 0;
        _owner = null;
    }

    private void EnsureDeliveryCapacity(int minimumLength)
    {
        if (_deliveries.Length >= minimumLength)
        {
            return;
        }

        MqttZeroDeliveryCandidate[] replacement =
            ArrayPool<MqttZeroDeliveryCandidate>.Shared.Rent(minimumLength);
        if (_deliveryCount > 0)
        {
            Array.Copy(_deliveries, replacement, _deliveryCount);
        }

        if (_deliveries.Length > 0)
        {
            ArrayPool<MqttZeroDeliveryCandidate>.Shared.Return(_deliveries);
        }

        _deliveries = replacement;
    }

    private static bool TryGetBestSubscription(
        MqttZeroSharedRouteEntry entry,
        ReadOnlySpan<byte> topic,
        MqttZeroSessionState? sender,
        [NotNullWhen(true)] out MqttZeroSubscription? subscription,
        out ReadOnlyMemory<uint> subscriptionIdentifiers)
    {
        subscription = null;
        List<uint>? identifiers = null;
        for (int index = 0; index < entry.Subscriptions.Length; index++)
        {
            MqttZeroSubscription candidate = entry.Subscriptions[index];
            if ((candidate.NoLocal && ReferenceEquals(entry.Session, sender)) ||
                !MqttZeroTopicMatcher.Matches(candidate.MatchFilter, topic))
            {
                continue;
            }

            if (subscription == null ||
                candidate.QualityOfServiceLevel > subscription.QualityOfServiceLevel)
            {
                subscription = candidate;
            }

            if (candidate.SubscriptionIdentifier.HasValue)
            {
                identifiers ??= [];
                identifiers.Add(candidate.SubscriptionIdentifier.Value);
            }
        }

        subscriptionIdentifiers = identifiers == null
            ? ReadOnlyMemory<uint>.Empty
            : identifiers.ToArray();
        return subscription != null;
    }

    private static ReadOnlyMemory<uint> ToIdentifiers(
        MqttZeroSubscription subscription)
    {
        return subscription.SubscriptionIdentifiers;
    }

    private static ReadOnlyMemory<uint> AppendIdentifier(
        ReadOnlyMemory<uint> identifiers,
        uint? identifier)
    {
        if (!identifier.HasValue)
        {
            return identifiers;
        }

        uint[] values = new uint[identifiers.Length + 1];
        identifiers.Span.CopyTo(values);
        values[^1] = identifier.Value;
        return values;
    }

    private void EnsureBestCapacity()
    {
        if (_bestSessions.Length == 0)
        {
            _bestCapacity = 16;
            _bestSessions = ArrayPool<MqttZeroSessionState?>.Shared.Rent(16);
            _bestDeliveryIndexes = ArrayPool<int>.Shared.Rent(16);
            _bestEpochs = ArrayPool<int>.Shared.Rent(16);
            Array.Clear(_bestSessions, 0, _bestSessions.Length);
            Array.Clear(_bestEpochs, 0, _bestEpochs.Length);
        }
    }

    private void GrowBestTable()
    {
        int capacity = _bestCapacity * 2;
        MqttZeroSessionState?[] sessions =
            ArrayPool<MqttZeroSessionState?>.Shared.Rent(capacity);
        int[] deliveryIndexes = ArrayPool<int>.Shared.Rent(capacity);
        int[] epochs = ArrayPool<int>.Shared.Rent(capacity);
        Array.Clear(sessions, 0, sessions.Length);
        Array.Clear(epochs, 0, epochs.Length);
        int epoch = _epoch;
        if (epoch == int.MaxValue)
        {
            _epoch = 1;
            epoch = 1;
            Array.Clear(_bestEpochs);
        }

        int mask = capacity - 1;
        for (int source = 0; source < _bestCapacity; source++)
        {
            MqttZeroSessionState? session = _bestSessions[source];
            if (session == null || _bestEpochs[source] != epoch)
            {
                continue;
            }

            int slot = RuntimeHelpers.GetHashCode(session) & mask;
            while (sessions[slot] != null)
            {
                slot = (slot + 1) & mask;
            }

            sessions[slot] = session;
            deliveryIndexes[slot] = _bestDeliveryIndexes[source];
            epochs[slot] = epoch;
        }

        ReturnBestArrays();
        _bestSessions = sessions;
        _bestDeliveryIndexes = deliveryIndexes;
        _bestEpochs = epochs;
        _bestCapacity = capacity;
    }

    private void ReturnBestArrays()
    {
        if (_bestSessions.Length > 0)
        {
            ArrayPool<MqttZeroSessionState?>.Shared.Return(_bestSessions, true);
            ArrayPool<int>.Shared.Return(_bestDeliveryIndexes);
            ArrayPool<int>.Shared.Return(_bestEpochs);
            _bestSessions = [];
            _bestDeliveryIndexes = [];
            _bestEpochs = [];
            _bestCapacity = 0;
        }
    }
}
