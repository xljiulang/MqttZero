using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using System.Text;

namespace MqttZero.Server.Broker;

public sealed partial class MqttZeroBroker
{
    private async ValueTask CommitSubscriptionsAsync(
        MqttZeroSessionState session,
        List<MqttZeroSubscription> original,
        List<MqttZeroSubscription> candidate,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(original);
        ArgumentNullException.ThrowIfNull(candidate);
        List<MqttZeroSubscription> removed = [];
        List<MqttZeroSubscription> added = [];
        BuildSubscriptionDiff(original, candidate, removed, added);
        await SaveSessionSubscriptionsAsync(
            session,
            candidate,
            cancellationToken).ConfigureAwait(false);
        await _routeMutationGate.WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        try
        {
            MqttZeroRouteEpoch current = _routeEpochs.Current;
            MqttZeroRouteEpoch replacement = current;
            for (int index = 0; index < removed.Count; index++)
            {
                MqttZeroSubscription subscription = removed[index];
                replacement = replacement.Remove(session, subscription);
                ReleaseSharedGroupReference(subscription);
            }

            for (int index = 0; index < added.Count; index++)
            {
                MqttZeroSubscription subscription = added[index];
                replacement = replacement.Add(session, subscription);
                RetainSharedGroupReference(subscription);
            }

            session.Subscriptions.Replace(candidate);

            _routeEpochs.Publish(current, replacement);
        }
        finally
        {
            _routeMutationGate.Release();
        }
    }

    private async ValueTask RemoveSessionSubscriptionsFromRouteAsync(
        MqttZeroSessionState session)
    {
        MqttZeroSubscription[] subscriptions =
            session.Subscriptions.Snapshot();
        if (subscriptions.Length == 0)
        {
            return;
        }

        await _routeMutationGate.WaitAsync(CancellationToken.None)
            .ConfigureAwait(false);
        try
        {
            MqttZeroRouteEpoch current = _routeEpochs.Current;
            MqttZeroRouteEpoch replacement = current;
            for (int index = 0; index < subscriptions.Length; index++)
            {
                replacement = replacement.Remove(
                    session,
                    subscriptions[index]);
                ReleaseSharedGroupReference(subscriptions[index]);
            }

            _routeEpochs.Publish(current, replacement);
        }
        finally
        {
            _routeMutationGate.Release();
        }
    }

    private async ValueTask AddSessionSubscriptionsToRouteAsync(
        MqttZeroSessionState session)
    {
        MqttZeroSubscription[] subscriptions =
            session.Subscriptions.Snapshot();
        if (subscriptions.Length == 0)
        {
            return;
        }

        await _routeMutationGate.WaitAsync(CancellationToken.None)
            .ConfigureAwait(false);
        try
        {
            MqttZeroRouteEpoch current = _routeEpochs.Current;
            MqttZeroRouteEpoch replacement = current;
            for (int index = 0; index < subscriptions.Length; index++)
            {
                replacement = replacement.Add(
                    session,
                    subscriptions[index]);
                RetainSharedGroupReference(subscriptions[index]);
            }

            _routeEpochs.Publish(current, replacement);
        }
        finally
        {
            _routeMutationGate.Release();
        }
    }

    internal async ValueTask ResetRouteEpochFromSessionsAsync()
    {
        MqttZeroRouteEpoch epoch =
            MqttZeroRouteEpochManager.BuildFromSessions(
                _sessions.Values);
        await _routeMutationGate.WaitAsync().ConfigureAwait(false);
        try
        {
            _routeEpochs.Reset(epoch);
            RebuildSharedGroupReferences();
        }
        finally
        {
            _routeMutationGate.Release();
        }
    }

    private void RebuildSharedGroupReferences()
    {
        _sharedGroupReferences.Clear();
        foreach (MqttZeroSessionState session in _sessions.Values)
        {
            MqttZeroSubscription[] subscriptions =
                session.Subscriptions.Snapshot();
            for (int index = 0;
                index < subscriptions.Length;
                index++)
            {
                RetainSharedGroupReference(
                    subscriptions[index]);
            }
        }

        HashSet<MqttZeroSharedGroupKey> keys =
            [.. _sharedGroupReferences.Keys];
        _sharedGroupCursors.PruneTo(keys);
    }

    private void RetainSharedGroupReference(
        MqttZeroSubscription subscription)
    {
        if (!subscription.IsShared)
        {
            return;
        }

        MqttZeroSharedGroupKey key =
            MqttZeroSharedGroupKey.Create(subscription);
        _sharedGroupReferences[key] =
            _sharedGroupReferences.TryGetValue(key, out int count)
                ? count + 1
                : 1;
    }

    private void ReleaseSharedGroupReference(
        MqttZeroSubscription subscription)
    {
        if (!subscription.IsShared)
        {
            return;
        }

        MqttZeroSharedGroupKey key =
            MqttZeroSharedGroupKey.Create(subscription);
        if (!_sharedGroupReferences.TryGetValue(key, out int count))
        {
            return;
        }

        if (count <= 1)
        {
            _sharedGroupReferences.Remove(key);
            _sharedGroupCursors.Remove(key);
            return;
        }

        _sharedGroupReferences[key] = count - 1;
    }

    private static void BuildSubscriptionDiff(
        List<MqttZeroSubscription> original,
        List<MqttZeroSubscription> candidate,
        List<MqttZeroSubscription> removed,
        List<MqttZeroSubscription> added)
    {
        Dictionary<string, MqttZeroSubscription> originalsByFilter =
            IndexByFilter(original);
        Dictionary<string, MqttZeroSubscription> candidatesByFilter =
            IndexByFilter(candidate);
        for (int index = 0; index < original.Count; index++)
        {
            MqttZeroSubscription subscription = original[index];
            if (!candidatesByFilter.TryGetValue(
                    GetFilterKey(subscription),
                    out MqttZeroSubscription? match) ||
                !HasSameRouteSettings(match, subscription))
            {
                removed.Add(subscription);
            }
        }

        for (int index = 0; index < candidate.Count; index++)
        {
            MqttZeroSubscription subscription = candidate[index];
            if (!originalsByFilter.TryGetValue(
                    GetFilterKey(subscription),
                    out MqttZeroSubscription? match) ||
                !HasSameRouteSettings(match, subscription))
            {
                added.Add(subscription);
            }
        }
    }

    private static Dictionary<string, MqttZeroSubscription> IndexByFilter(
        List<MqttZeroSubscription> subscriptions)
    {
        Dictionary<string, MqttZeroSubscription> result = new(
            subscriptions.Count,
            StringComparer.Ordinal);
        for (int index = 0; index < subscriptions.Count; index++)
        {
            MqttZeroSubscription subscription = subscriptions[index];
            result[GetFilterKey(subscription)] = subscription;
        }

        return result;
    }

    private static string GetFilterKey(
        MqttZeroSubscription subscription)
    {
        return Encoding.UTF8.GetString(subscription.TopicFilter);
    }

    private static bool HasSameRouteSettings(
        MqttZeroSubscription left,
        MqttZeroSubscription right)
    {
        return left.QualityOfServiceLevel == right.QualityOfServiceLevel &&
            left.NoLocal == right.NoLocal &&
            left.RetainAsPublished == right.RetainAsPublished &&
            left.SubscriptionIdentifier == right.SubscriptionIdentifier;
    }
}
