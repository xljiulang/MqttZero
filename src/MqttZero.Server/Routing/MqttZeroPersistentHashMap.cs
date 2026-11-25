using System.Runtime.CompilerServices;

namespace MqttZero.Server.Routing;

internal sealed class MqttZeroPersistentHashMap<TKey, TValue>
{
    private const int FrameCapacity = 16;

    private readonly IEqualityComparer<TKey> _comparer;
    private readonly MqttZeroPersistentHashMapNode<TKey, TValue>? _root;

    public MqttZeroPersistentHashMap(IEqualityComparer<TKey> comparer)
        : this(comparer, null)
    {
    }

    private MqttZeroPersistentHashMap(
        IEqualityComparer<TKey> comparer,
        MqttZeroPersistentHashMapNode<TKey, TValue>? root)
    {
        _comparer = comparer ??
            throw new ArgumentNullException(nameof(comparer));
        _root = root;
    }

    public int Count
    {
        get
        {
            return _root?.Count ?? 0;
        }
    }

    internal MqttZeroPersistentHashMapNode<TKey, TValue>? Root
    {
        get
        {
            return _root;
        }
    }

    public static MqttZeroPersistentHashMap<TKey, TValue> Create(
        IEqualityComparer<TKey> comparer)
    {
        return new MqttZeroPersistentHashMap<TKey, TValue>(comparer);
    }

    public bool TryGetValue(TKey key, out TValue value)
    {
        if (_root == null)
        {
            value = default!;
            return false;
        }

        return _root.TryGetValue(
            key,
            _comparer.GetHashCode(key!),
            0,
            _comparer,
            out value);
    }

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
                    _comparer.GetHashCode(key!))
                : _root.Set(
                    key,
                    value,
                    _comparer.GetHashCode(key!),
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
            _comparer.GetHashCode(key!),
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

    public Enumerator GetEnumerator()
    {
        return new Enumerator(_root);
    }

    internal ref struct Enumerator
    {
        private MqttZeroPersistentHashMapEntry<TKey, TValue> _current;
        private NodeBuffer _nodes;
        private IndexBuffer _indexes;
        private int _stackCount;

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

        public MqttZeroPersistentHashMapEntry<TKey, TValue> Current
        {
            get
            {
                return _current;
            }
        }

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

    [InlineArray(FrameCapacity)]
    private struct NodeBuffer
    {
        private MqttZeroPersistentHashMapNode<TKey, TValue>? _element;
    }

    [InlineArray(FrameCapacity)]
    private struct IndexBuffer
    {
        private int _element;
    }
}
