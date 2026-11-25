using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;

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
        await _routeMutationGate.WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        try
        {
            MqttZeroRouteEpoch current = _routeEpochs.Current;
            MqttZeroRouteEpoch replacement = current;
            for (int index = 0; index < original.Count; index++)
            {
                replacement = replacement.Remove(
                    session,
                    original[index]);
            }

            for (int index = 0; index < candidate.Count; index++)
            {
                replacement = replacement.Add(
                    session,
                    candidate[index]);
            }

            await SaveSessionSubscriptionsAsync(
                session,
                candidate,
                cancellationToken).ConfigureAwait(false);
            lock (session.SyncRoot)
            {
                session.Subscriptions.Clear();
                session.Subscriptions.AddRange(candidate);
            }

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
        MqttZeroSubscription[] subscriptions;
        lock (session.SyncRoot)
        {
            if (session.Subscriptions.Count == 0)
            {
                return;
            }

            subscriptions = [.. session.Subscriptions];
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
        MqttZeroSubscription[] subscriptions;
        lock (session.SyncRoot)
        {
            if (session.Subscriptions.Count == 0)
            {
                return;
            }

            subscriptions = [.. session.Subscriptions];
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
            }

            _routeEpochs.Publish(current, replacement);
        }
        finally
        {
            _routeMutationGate.Release();
        }
    }

    internal void ResetRouteEpochFromSessions()
    {
        MqttZeroRouteEpoch epoch =
            MqttZeroRouteEpochManager.BuildFromSessions(
                _sessions.Values);
        _routeEpochs.Reset(epoch);
    }
}
