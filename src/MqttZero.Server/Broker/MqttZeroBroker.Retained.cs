using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Retained;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Text;

namespace MqttZero.Server.Broker;

/// <summary>
/// MQTT 5 Broker 主体，按连接、会话、路由、投递与生命周期拆成多个分部文件。
/// </summary>
public sealed partial class MqttZeroBroker
{
    /// <summary>
    /// 存储保留消息。
    /// </summary>
    /// <param name="lease">租约。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask StoreRetainedMessageAsync(
        MqttZeroMessageLease lease,
        CancellationToken cancellationToken)
    {
        if (!_retainedMessageStore.IsEnabled)
        {
            await StoreRetainedMessageInMemoryAsync(
                lease,
                cancellationToken).ConfigureAwait(false);
            return;
        }

        string topicKey = Encoding.UTF8.GetString(lease.Topic.Span);
        DateTimeOffset? expiresAt = lease.ExpiresAt;
        MqttZeroRetainedMessage? removed = null;
        MqttZeroRetainedMessage? replaced = null;
        bool stored = false;
        await _retainedMutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (lease.PayloadSequence.IsEmpty)
            {
                await DeleteRetainedAsync(topicKey, cancellationToken)
                    .ConfigureAwait(false);
                removed = RemoveRetainedMessageCore(topicKey);
            }
            else
            {
                lease.EnsureOwned();
                ValidateRetainedCapacity(
                    topicKey,
                    lease.TopicSequence.Length +
                        lease.PayloadSequence.Length);
                await SaveRetainedAsync(
                    lease,
                    expiresAt,
                    cancellationToken).ConfigureAwait(false);
                replaced = StoreRetainedMessageInMemoryCore(
                    lease,
                    topicKey,
                    expiresAt);
                stored = true;
            }
        }
        finally
        {
            _retainedMutationGate.Release();
        }

        if (removed != null)
        {
            try
            {
                await CancelRetainedDeadlineAsync(
                    topicKey,
                    removed,
                    cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                removed.Release();
            }

            return;
        }

        if (!stored)
        {
            return;
        }

        await ScheduleRetainedDeadlineAsync(
            topicKey,
            expiresAt,
            cancellationToken).ConfigureAwait(false);
        if (replaced != null)
        {
            try
            {
                await CancelRetainedDeadlineAsync(
                    topicKey,
                    replaced,
                    cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                replaced.Release();
            }
        }
    }

    /// <summary>
    /// 存储保留消息In内存。
    /// </summary>
    /// <param name="lease">租约。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask StoreRetainedMessageInMemoryAsync(
        MqttZeroMessageLease lease,
        CancellationToken cancellationToken)
    {
        string topicKey = Encoding.UTF8.GetString(lease.Topic.Span);
        if (lease.PayloadSequence.IsEmpty)
        {
            await RemoveRetainedMessageAsync(
                topicKey,
                cancellationToken).ConfigureAwait(false);
            return;
        }

        lease.EnsureOwned();
        DateTimeOffset? expiresAt = lease.ExpiresAt;
        MqttZeroRetainedMessage? replaced =
            StoreRetainedMessageInMemoryCore(
                lease,
                topicKey,
                expiresAt);
        await ScheduleRetainedDeadlineAsync(
            topicKey,
            expiresAt,
            cancellationToken).ConfigureAwait(false);
        if (replaced != null)
        {
            await CancelRetainedDeadlineAsync(
                topicKey,
                replaced,
                cancellationToken).ConfigureAwait(false);
            replaced.Release();
        }
    }

    /// <summary>
    /// 存储保留消息In内存。
    /// </summary>
    /// <param name="lease">租约。</param>
    /// <param name="topicKey">主题键。</param>
    /// <param name="expiresAt">对应的 expiresAt。</param>
    /// <returns>对应的结果。</returns>
    private MqttZeroRetainedMessage? StoreRetainedMessageInMemoryCore(
        MqttZeroMessageLease lease,
        string topicKey,
        DateTimeOffset? expiresAt)
    {
        MqttZeroRetainedMessage? replaced;
        lock (_retainedGate)
        {
            _retainedMessages.TryGetValue(
                topicKey,
                out MqttZeroRetainedMessage? existing);
            if (existing == null &&
                _options.MaxRetainedMessages > 0 &&
                _retainedMessages.Count >= _options.MaxRetainedMessages)
            {
                throw new InvalidOperationException(
                    "The retained message count limit was reached.");
            }

            long previousBytes = existing?.ByteLength ?? 0;
            long replacementBytes =
                lease.TopicSequence.Length +
                lease.PropertiesSequence.Length +
                lease.PayloadSequence.Length;
            long retainedBytes =
                _retainedBytes - previousBytes + replacementBytes;
            if (retainedBytes > _options.MaxRetainedBytes)
            {
                throw new InvalidOperationException(
                    "The retained message byte limit was reached.");
            }

            MqttZeroRetainedMessage replacement = new(
                lease,
                lease.QualityOfServiceLevel,
                expiresAt);
            MqttZeroRetainedTopicTrie currentTrie =
                Volatile.Read(ref _retainedTopicTrie);
            MqttZeroRetainedTopicTrie replacementTrie =
                currentTrie.Set(
                    lease.Topic.Span,
                    replacement);
            _retainedMessages[topicKey] = replacement;
            Volatile.Write(ref _retainedTopicTrie, replacementTrie);
            _retainedBytes = retainedBytes;
            replaced = existing;
        }

        return replaced;
    }

    /// <summary>
    /// 校验保留容量。
    /// </summary>
    /// <param name="topicKey">主题键。</param>
    /// <param name="replacementBytes">replacement字节数。</param>
    private void ValidateRetainedCapacity(string topicKey, long replacementBytes)
    {
        lock (_retainedGate)
        {
            _retainedMessages.TryGetValue(topicKey, out MqttZeroRetainedMessage? existing);
            if (existing == null &&
                _options.MaxRetainedMessages > 0 &&
                _retainedMessages.Count >= _options.MaxRetainedMessages)
            {
                throw new InvalidOperationException("The retained message count limit was reached.");
            }

            long previousBytes = existing?.ByteLength ?? 0;
            if (_retainedBytes - previousBytes + replacementBytes > _options.MaxRetainedBytes)
            {
                throw new InvalidOperationException("The retained message byte limit was reached.");
            }
        }
    }

    /// <summary>
    /// 移除保留消息。
    /// </summary>
    /// <param name="topicKey">主题键。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask RemoveRetainedMessageAsync(
        string topicKey,
        CancellationToken cancellationToken)
    {
        MqttZeroRetainedMessage? removed =
            RemoveRetainedMessageCore(topicKey);
        if (removed == null)
        {
            return;
        }

        try
        {
            await CancelRetainedDeadlineAsync(
                topicKey,
                removed,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            removed.Release();
        }
    }

    /// <summary>
    /// 移除保留消息。
    /// </summary>
    /// <param name="topicKey">主题键。</param>
    /// <returns>对应的结果。</returns>
    private MqttZeroRetainedMessage? RemoveRetainedMessageCore(
        string topicKey)
    {
        MqttZeroRetainedMessage? removed = null;
        lock (_retainedGate)
        {
            if (_retainedMessages.TryRemove(topicKey, out removed))
            {
                MqttZeroRetainedTopicTrie currentTrie =
                    Volatile.Read(ref _retainedTopicTrie);
                Volatile.Write(
                    ref _retainedTopicTrie,
                    currentTrie.Remove(removed.Topic.Span));
                _retainedBytes -= removed.ByteLength;
            }
        }

        return removed;
    }

    /// <summary>
    /// 匹配保留消息。
    /// </summary>
    /// <param name="filter">过滤器。</param>
    /// <param name="results">结果。</param>
    private void MatchRetainedMessages(
        ReadOnlySpan<byte> filter,
        List<MqttZeroRetainedMessage> results)
    {
        lock (_retainedGate)
        {
            MqttZeroRetainedTopicTrie trie =
                Volatile.Read(ref _retainedTopicTrie);
            trie.Match(filter, results);
            for (int index = 0; index < results.Count; index++)
            {
                results[index].Retain();
            }
        }
    }

    /// <summary>
    /// 释放保留消息。
    /// </summary>
    /// <param name="messages">消息。</param>
    private static void ReleaseRetainedMessages(
        List<MqttZeroRetainedMessage> messages)
    {
        for (int index = 0; index < messages.Count; index++)
        {
            messages[index].Release();
        }
    }

}
