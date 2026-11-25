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
/// Implements retained message storage and removal.
/// </summary>
public sealed partial class MqttZeroBroker
{
    private async ValueTask StoreRetainedMessageAsync(
        MqttZeroMessageLease lease,
        CancellationToken cancellationToken)
    {
        if (!_retainedMessageStore.IsEnabled)
        {
            StoreRetainedMessageInMemory(lease);
            return;
        }

        await _retainedMutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            string topicKey = Encoding.UTF8.GetString(lease.Topic.Span);
            if (lease.PayloadSequence.IsEmpty)
            {
                await DeleteRetainedAsync(topicKey, cancellationToken)
                    .ConfigureAwait(false);
                RemoveRetainedMessage(topicKey);
                return;
            }

            lease.EnsureOwned();
            ValidateRetainedCapacity(topicKey, lease.TopicSequence.Length + lease.PayloadSequence.Length);
            DateTimeOffset? expiresAt = lease.ExpiresAt;
            await SaveRetainedAsync(lease, expiresAt, cancellationToken).ConfigureAwait(false);
            StoreRetainedMessageInMemory(lease, topicKey, expiresAt);
        }
        finally
        {
            _retainedMutationGate.Release();
        }
    }

    private void StoreRetainedMessageInMemory(MqttZeroMessageLease lease)
    {
        string topicKey = Encoding.UTF8.GetString(lease.Topic.Span);
        if (lease.PayloadSequence.IsEmpty)
        {
            RemoveRetainedMessage(topicKey);
            return;
        }

        lease.EnsureOwned();
        DateTimeOffset? expiresAt = lease.ExpiresAt;
        StoreRetainedMessageInMemory(lease, topicKey, expiresAt);
    }

    private void StoreRetainedMessageInMemory(
        MqttZeroMessageLease lease,
        string topicKey,
        DateTimeOffset? expiresAt)
    {
        MqttZeroRetainedMessage? replaced;
        lock (_sessionGate)
        {
            _retainedMessages.TryGetValue(topicKey, out MqttZeroRetainedMessage? existing);
            if (existing == null &&
                _options.MaxRetainedMessages > 0 &&
                _retainedMessages.Count >= _options.MaxRetainedMessages)
            {
                throw new InvalidOperationException("The retained message count limit was reached.");
            }

            long previousBytes = existing?.ByteLength ?? 0;
            long replacementBytes =
                lease.TopicSequence.Length +
                lease.PropertiesSequence.Length +
                lease.PayloadSequence.Length;
            long retainedBytes = _retainedBytes - previousBytes + replacementBytes;
            if (retainedBytes > _options.MaxRetainedBytes)
            {
                throw new InvalidOperationException("The retained message byte limit was reached.");
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

        replaced?.Release();
    }

    private void ValidateRetainedCapacity(string topicKey, long replacementBytes)
    {
        lock (_sessionGate)
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

    private void RemoveRetainedMessage(string topicKey)
    {
        MqttZeroRetainedMessage? removed = null;
        lock (_sessionGate)
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

        removed?.Release();
    }

    private void MatchRetainedMessages(
        ReadOnlySpan<byte> filter,
        List<MqttZeroRetainedMessage> results)
    {
        lock (_sessionGate)
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

    private static void ReleaseRetainedMessages(
        List<MqttZeroRetainedMessage> messages)
    {
        for (int index = 0; index < messages.Count; index++)
        {
            messages[index].Release();
        }
    }

}
