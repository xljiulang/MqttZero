using MqttZero.Memory;
using System.Buffers;

namespace MqttZero.Server.Clients;

internal sealed class MqttZeroTopicAliasTable : IDisposable
{
    private readonly MqttZeroBufferPool _bufferPool;
    private Dictionary<ushort, MqttZeroPooledTopicAlias>? _largeAliases;
    private MqttZeroPooledTopicAlias?[]? _smallAliases;

    public MqttZeroTopicAliasTable(MqttZeroBufferPool bufferPool)
    {
        _bufferPool = bufferPool ??
            throw new ArgumentNullException(nameof(bufferPool));
    }

    public void Dispose()
    {
        if (_largeAliases != null)
        {
            foreach (MqttZeroPooledTopicAlias alias in
                _largeAliases.Values)
            {
                _bufferPool.Return(alias.Buffer);
            }

            _largeAliases = null;
        }

        if (_smallAliases != null)
        {
            for (int index = 0; index < _smallAliases.Length; index++)
            {
                MqttZeroPooledTopicAlias? alias = _smallAliases[index];
                if (alias != null)
                {
                    _bufferPool.Return(alias.Buffer);
                    _smallAliases[index] = null;
                }
            }

            _smallAliases = null;
        }
    }

    public void Set(ushort alias, ReadOnlySequence<byte> topic)
    {
        int length = checked((int)topic.Length);
        if (alias <= 256)
        {
            _smallAliases ??= new MqttZeroPooledTopicAlias?[257];
            SetEntry(ref _smallAliases[alias], topic, length);
            return;
        }

        _largeAliases ??= [];
        _largeAliases.TryGetValue(
            alias,
            out MqttZeroPooledTopicAlias? existing);
        MqttZeroPooledTopicAlias replacement = CreateEntry(
            existing,
            topic,
            length);
        _largeAliases[alias] = replacement;
    }

    public bool TryGet(ushort alias, out ReadOnlyMemory<byte> topic)
    {
        MqttZeroPooledTopicAlias? entry;
        if (alias <= 256)
        {
            entry = _smallAliases?[alias];
        }
        else
        {
            entry = _largeAliases?.GetValueOrDefault(alias);
        }

        if (entry != null)
        {
            topic = entry.Buffer.AsMemory(0, entry.Length);
            return true;
        }

        topic = ReadOnlyMemory<byte>.Empty;
        return false;
    }

    private MqttZeroPooledTopicAlias CreateEntry(
        MqttZeroPooledTopicAlias? existing,
        ReadOnlySequence<byte> topic,
        int length)
    {
        MqttZeroPooledTopicAlias entry =
            existing != null && existing.Buffer.Length >= length
                ? existing
                : new MqttZeroPooledTopicAlias(
                    _bufferPool.Rent(length),
                    length);
        if (existing != null &&
            !ReferenceEquals(existing, entry))
        {
            _bufferPool.Return(existing.Buffer);
        }

        topic.CopyTo(entry.Buffer);
        entry.Length = length;
        return entry;
    }

    private void SetEntry(
        ref MqttZeroPooledTopicAlias? entry,
        ReadOnlySequence<byte> topic,
        int length)
    {
        if (entry == null || entry.Buffer.Length < length)
        {
            if (entry != null)
            {
                _bufferPool.Return(entry.Buffer);
            }

            entry = new MqttZeroPooledTopicAlias(
                _bufferPool.Rent(length),
                length);
        }

        topic.CopyTo(entry.Buffer);
        entry.Length = length;
    }
}
