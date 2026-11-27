using MqttZero.Memory;
using System.Buffers;

namespace MqttZero.Server.Clients;

/// <summary>
/// 入站主题别名到主题的映射表。
/// </summary>
internal sealed class MqttZeroTopicAliasTable : IDisposable
{
    private readonly MqttZeroBufferPool _bufferPool;
    private Dictionary<ushort, MqttZeroPooledTopicAlias>? _largeAliases;
    private MqttZeroPooledTopicAlias?[]? _smallAliases;

    /// <summary>
    /// 初始化 MqttZeroTopicAliasTable 的新实例。
    /// </summary>
    /// <param name="bufferPool">缓冲区池。</param>
    public MqttZeroTopicAliasTable(MqttZeroBufferPool bufferPool)
    {
        _bufferPool = bufferPool ??
            throw new ArgumentNullException(nameof(bufferPool));
    }

    /// <summary>
    /// 释放。
    /// </summary>
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

    /// <summary>
    /// 设置。
    /// </summary>
    /// <param name="alias">别名。</param>
    /// <param name="topic">主题。</param>
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

    /// <summary>
    /// 尝试Get。
    /// </summary>
    /// <param name="alias">别名。</param>
    /// <param name="topic">主题。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
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

    /// <summary>
    /// 创建条目。
    /// </summary>
    /// <param name="existing">对应的 existing。</param>
    /// <param name="topic">主题。</param>
    /// <param name="length">长度。</param>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// 设置条目。
    /// </summary>
    /// <param name="entry">条目。</param>
    /// <param name="topic">主题。</param>
    /// <param name="length">长度。</param>
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
