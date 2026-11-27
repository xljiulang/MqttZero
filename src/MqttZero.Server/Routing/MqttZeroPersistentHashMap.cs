using System.Runtime.CompilerServices;

namespace MqttZero.Server.Routing;

/// <summary>
/// 不可变持久化哈希映射。
/// </summary>
internal sealed class MqttZeroPersistentHashMap<TKey, TValue>
    where TKey : notnull
{
    private const int FrameCapacity = 16;

    private readonly IEqualityComparer<TKey> _comparer;
    private readonly MqttZeroPersistentHashMapNode<TKey, TValue>? _root;

    /// <summary>
    /// 初始化 MqttZeroPersistentHashMap 的新实例。
    /// </summary>
    /// <param name="comparer">对应的 comparer。</param>
    public MqttZeroPersistentHashMap(IEqualityComparer<TKey> comparer)
        : this(comparer, null)
    {
    }

    /// <summary>
    /// 初始化 MqttZeroPersistentHashMap 的新实例。
    /// </summary>
    /// <param name="comparer">对应的 comparer。</param>
    /// <param name="root">对应的 root。</param>
    private MqttZeroPersistentHashMap(
        IEqualityComparer<TKey> comparer,
        MqttZeroPersistentHashMapNode<TKey, TValue>? root)
    {
        _comparer = comparer ??
            throw new ArgumentNullException(nameof(comparer));
        _root = root;
    }

    /// <summary>
    /// Count 的值。
    /// </summary>
    public int Count
    {
        get
        {
            return _root?.Count ?? 0;
        }
    }

    /// <summary>
    /// Root 的值。
    /// </summary>
    internal MqttZeroPersistentHashMapNode<TKey, TValue>? Root
    {
        get
        {
            return _root;
        }
    }

    /// <summary>
    /// 创建。
    /// </summary>
    /// <param name="comparer">对应的 comparer。</param>
    /// <returns>对应的结果。</returns>
    public static MqttZeroPersistentHashMap<TKey, TValue> Create(
        IEqualityComparer<TKey> comparer)
    {
        return new MqttZeroPersistentHashMap<TKey, TValue>(comparer);
    }

    /// <summary>
    /// 尝试Get值。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="value">值。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public bool TryGetValue(TKey key, out TValue value)
    {
        if (_root == null)
        {
            value = default!;
            return false;
        }

        return _root.TryGetValue(
            key,
            _comparer.GetHashCode(key),
            0,
            _comparer,
            out value);
    }

    /// <summary>
    /// 设置。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="value">值。</param>
    /// <param name="added">对应的 added。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroPersistentHashMap<TKey, TValue> Set(
        TKey key,
        TValue value,
        out bool added)
    {
        bool changed = false;
        MqttZeroPersistentHashMapNode<TKey, TValue> root =
            _root == null
                ? new MqttZeroPersistentHashMapLeafNode<TKey, TValue>(
                    key,
                    value,
                    _comparer.GetHashCode(key))
                : _root.Set(
                    key,
                    value,
                    _comparer.GetHashCode(key),
                    0,
                    _comparer,
                    ref changed);
        added = _root == null || changed;
        return ReferenceEquals(root, _root)
            ? this
            : new MqttZeroPersistentHashMap<TKey, TValue>(
                _comparer,
                root);
    }

    /// <summary>
    /// 移除。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="removed">对应的 removed。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroPersistentHashMap<TKey, TValue> Remove(
        TKey key,
        out bool removed)
    {
        if (_root == null)
        {
            removed = false;
            return this;
        }

        bool changed = false;
        MqttZeroPersistentHashMapNode<TKey, TValue>? root = _root.Remove(
            key,
            _comparer.GetHashCode(key),
            0,
            _comparer,
            ref changed);
        removed = changed;
        return !changed || ReferenceEquals(root, _root)
            ? this
            : new MqttZeroPersistentHashMap<TKey, TValue>(
                _comparer,
                root);
    }

    /// <summary>
    /// 获取Enumerator。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public Enumerator GetEnumerator()
    {
        return new Enumerator(_root);
    }

    /// <summary>
    /// 集合的只读枚举器。
    /// </summary>
    internal ref struct Enumerator
    {
        private MqttZeroPersistentHashMapEntry<TKey, TValue> _current;
        private NodeBuffer _nodes;
        private IndexBuffer _indexes;
        private int _stackCount;

        /// <summary>
        /// 初始化 Enumerator 的新实例。
        /// </summary>
        /// <param name="root">对应的 root。</param>
        public Enumerator(MqttZeroPersistentHashMapNode<TKey, TValue>? root)
        {
            _current = default;
            _nodes = default;
            _indexes = default;
            _stackCount = 0;
            if (root != null)
            {
                Push(root);
            }
        }

        /// <summary>
        /// Current 的值。
        /// </summary>
        public MqttZeroPersistentHashMapEntry<TKey, TValue> Current
        {
            get
            {
                return _current;
            }
        }

        /// <summary>
        /// Move下一个。
        /// </summary>
        /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
        public bool MoveNext()
        {
            while (_stackCount > 0)
            {
                int frame = _stackCount - 1;
                MqttZeroPersistentHashMapNode<TKey, TValue> node =
                    _nodes[frame]!;
                int index = _indexes[frame];
                if (node is MqttZeroPersistentHashMapLeafNode<TKey, TValue> leaf)
                {
                    _stackCount--;
                    _current = new MqttZeroPersistentHashMapEntry<TKey, TValue>(
                        leaf.Key,
                        leaf.Value);
                    return true;
                }

                if (node is MqttZeroPersistentHashMapCollisionNode<TKey, TValue>
                    collision)
                {
                    if (index < collision.Entries.Length)
                    {
                        _indexes[frame] = index + 1;
                        _current = collision.Entries[index];
                        return true;
                    }

                    _stackCount--;
                    continue;
                }

                MqttZeroPersistentHashMapBitmapNode<TKey, TValue> bitmap =
                    (MqttZeroPersistentHashMapBitmapNode<TKey, TValue>)node;
                if (index < bitmap.Children.Length)
                {
                    _indexes[frame] = index + 1;
                    Push(bitmap.Children[index]);
                    continue;
                }

                _stackCount--;
            }

            _current = default;
            return false;
        }

        /// <summary>
        /// 执行 Push 对应的处理。
        /// </summary>
        /// <param name="node">节点。</param>
        private void Push(MqttZeroPersistentHashMapNode<TKey, TValue> node)
        {
            if (_stackCount == FrameCapacity)
            {
                throw new InvalidOperationException(
                    "The persistent hash map exceeded its maximum trie depth.");
            }

            _nodes[_stackCount] = node;
            _indexes[_stackCount] = 0;
            _stackCount++;
        }
    }

    /// <summary>
    /// 持久化哈希映射构建时复用的节点缓冲区。
    /// </summary>
    [InlineArray(FrameCapacity)]
    private struct NodeBuffer
    {
        private MqttZeroPersistentHashMapNode<TKey, TValue>? _element;
    }

    /// <summary>
    /// 持久化哈希映射构建时复用的索引缓冲区。
    /// </summary>
    [InlineArray(FrameCapacity)]
    private struct IndexBuffer
    {
        private int _element;
    }
}
