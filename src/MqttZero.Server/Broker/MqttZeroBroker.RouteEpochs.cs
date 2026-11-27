using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using System.Text;

namespace MqttZero.Server.Broker;

/// <summary>
/// MQTT 5 Broker 主体，按连接、会话、路由、投递与生命周期拆成多个分部文件。
/// </summary>
public sealed partial class MqttZeroBroker
{
    /// <summary>
    /// Commit订阅。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="original">对应的 original。</param>
    /// <param name="candidate">对应的 candidate。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask CommitSubscriptionsAsync(
        MqttZeroSessionState session,
        List<MqttZeroSubscription> original,
        List<MqttZeroSubscription> candidate,
        CancellationToken cancellationToken)
    {
        EnsureSessionShard(
            session,
            nameof(CommitSubscriptionsAsync));
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

    /// <summary>
    /// 移除会话订阅FromRoute。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask RemoveSessionSubscriptionsFromRouteAsync(
        MqttZeroSessionState session)
    {
        EnsureSessionShard(
            session,
            nameof(RemoveSessionSubscriptionsFromRouteAsync));
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

    /// <summary>
    /// 添加会话订阅ToRoute。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask AddSessionSubscriptionsToRouteAsync(
        MqttZeroSessionState session)
    {
        EnsureSessionShard(
            session,
            nameof(AddSessionSubscriptionsToRouteAsync));
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

    /// <summary>
    /// 重置Route路由快照FromSessions。
    /// </summary>
    /// <returns>表示异步操作的任务。</returns>
    internal async ValueTask ResetRouteEpochFromSessionsAsync()
    {
        MqttZeroRouteEpoch epoch =
            MqttZeroRouteEpochManager.BuildFromSessions(
                _sessions.Values,
                _sharedGroupCursors);
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

    /// <summary>
    /// Rebuild共享组References。
    /// </summary>
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

    /// <summary>
    /// 保留共享组Reference。
    /// </summary>
    /// <param name="subscription">订阅。</param>
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

    /// <summary>
    /// 释放共享组Reference。
    /// </summary>
    /// <param name="subscription">订阅。</param>
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

    /// <summary>
    /// 构建订阅Diff。
    /// </summary>
    /// <param name="original">对应的 original。</param>
    /// <param name="candidate">对应的 candidate。</param>
    /// <param name="removed">对应的 removed。</param>
    /// <param name="added">对应的 added。</param>
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

    /// <summary>
    /// 索引过滤器。
    /// </summary>
    /// <param name="subscriptions">订阅。</param>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// 获取过滤器键。
    /// </summary>
    /// <param name="subscription">订阅。</param>
    /// <returns>对应的字符串。</returns>
    private static string GetFilterKey(
        MqttZeroSubscription subscription)
    {
        return Encoding.UTF8.GetString(subscription.TopicFilter);
    }

    /// <summary>
    /// 判断是否存在SameRoute设置。
    /// </summary>
    /// <param name="left">对应的 left。</param>
    /// <param name="right">对应的 right。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
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
