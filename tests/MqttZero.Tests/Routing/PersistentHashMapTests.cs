using MqttZero.Server.Routing;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace MqttZero.Tests.Routing;

public sealed class PersistentHashMapTests
{
    [Fact]
    public void SetAndRemovePreserveUnchangedSubtrees()
    {
        MqttZeroPersistentHashMap<string, int> empty =
            MqttZeroPersistentHashMap<string, int>.Create(
                StringComparer.Ordinal);
        MqttZeroPersistentHashMap<string, int> first =
            empty.Set("a", 1, out bool addedA);
        MqttZeroPersistentHashMap<string, int> second =
            first.Set("b", 2, out bool addedB);
        MqttZeroPersistentHashMap<string, int> removed =
            second.Remove("a", out bool removedA);

        Assert.True(addedA);
        Assert.True(addedB);
        Assert.True(removedA);
        Assert.Equal(2, second.Count);
        Assert.Equal(1, removed.Count);
        Assert.True(second.TryGetValue("a", out int value));
        Assert.Equal(1, value);
        Assert.False(removed.TryGetValue("a", out _));
        Assert.True(second.TryGetValue("b", out _));
    }

    [Fact]
    public void UpdatingExistingKeyDoesNotIncreaseCount()
    {
        MqttZeroPersistentHashMap<string, int> original =
            MqttZeroPersistentHashMap<string, int>.Create(
                StringComparer.Ordinal)
            .Set("key", 1, out _);

        MqttZeroPersistentHashMap<string, int> updated =
            original.Set("key", 2, out bool added);

        Assert.False(added);
        Assert.Equal(1, original.Count);
        Assert.Equal(1, updated.Count);
        Assert.True(original.TryGetValue("key", out int originalValue));
        Assert.Equal(1, originalValue);
        Assert.True(updated.TryGetValue("key", out int updatedValue));
        Assert.Equal(2, updatedValue);
    }

    [Fact]
    public void EnumeratorReturnsEveryEntryAfterHashCollisions()
    {
        MqttZeroPersistentHashMap<CollisionKey, int> map =
            MqttZeroPersistentHashMap<CollisionKey, int>.Create(
                CollisionKeyComparer.Instance);
        for (int index = 0; index < 100; index++)
        {
            map = map.Set(new CollisionKey(index), index, out _);
        }

        HashSet<int> values = [];
        foreach (MqttZeroPersistentHashMapEntry<CollisionKey, int> entry in map)
        {
            values.Add(entry.Value);
        }

        Assert.Equal(Enumerable.Range(0, 100), values.Order());
        Assert.Equal(100, map.Count);
    }

    [Fact]
    public void RemoveSupportsCollisionAndMissingKeys()
    {
        MqttZeroPersistentHashMap<CollisionKey, int> map =
            MqttZeroPersistentHashMap<CollisionKey, int>.Create(
                CollisionKeyComparer.Instance)
            .Set(new CollisionKey(1), 10, out _)
            .Set(new CollisionKey(2), 20, out _);

        MqttZeroPersistentHashMap<CollisionKey, int> removed =
            map.Remove(new CollisionKey(1), out bool removedExisting);
        MqttZeroPersistentHashMap<CollisionKey, int> unchanged =
            removed.Remove(new CollisionKey(3), out bool removedMissing);

        Assert.True(removedExisting);
        Assert.False(removedMissing);
        Assert.Equal(1, removed.Count);
        Assert.False(removed.TryGetValue(new CollisionKey(1), out _));
        Assert.True(removed.TryGetValue(new CollisionKey(2), out int value));
        Assert.Equal(20, value);
        Assert.Same(removed, unchanged);
    }

    [Fact]
    public void RandomOperationsMatchDictionary()
    {
        const int keySpace = 1_024;
        MqttZeroPersistentHashMap<string, int> map =
            MqttZeroPersistentHashMap<string, int>.Create(
                StringComparer.Ordinal);
        Dictionary<string, int> expected = new(StringComparer.Ordinal);
        Random random = new(42);
        for (int operation = 0; operation < 10_000; operation++)
        {
            string key = (
                random.Next(keySpace) *
                16_777_619 % keySpace)
                .ToString(CultureInfo.InvariantCulture);
            if ((operation & 1) == 0)
            {
                int value = random.Next();
                map = map.Set(key, value, out bool added);
                bool expectedAdded = !expected.ContainsKey(key);
                expected[key] = value;
                Assert.Equal(expectedAdded, added);
            }
            else
            {
                map = map.Remove(key, out bool removed);
                bool expectedRemoved = expected.Remove(key);
                Assert.Equal(expectedRemoved, removed);
            }
        }

        Assert.Equal(expected.Count, map.Count);
        for (int key = 0; key < keySpace; key++)
        {
            string text = key.ToString(CultureInfo.InvariantCulture);
            bool expectedFound = expected.TryGetValue(text, out int expectedValue);
            bool found = map.TryGetValue(text, out int actualValue);
            Assert.Equal(expectedFound, found);
            if (expectedFound)
            {
                Assert.Equal(expectedValue, actualValue);
            }
        }
    }

    [Fact]
    public void UpdatingOneBranchPreservesOtherBranchReferences()
    {
        HashKey firstKey = new(1, 1);
        HashKey secondKey = new(2, 2);
        MqttZeroPersistentHashMap<HashKey, int> original =
            MqttZeroPersistentHashMap<HashKey, int>.Create(
                HashKeyComparer.Instance)
            .Set(firstKey, 10, out _)
            .Set(secondKey, 20, out _);
        MqttZeroPersistentHashMapBitmapNode<HashKey, int> originalRoot =
            Assert.IsType<MqttZeroPersistentHashMapBitmapNode<HashKey, int>>(
                original.Root);
        MqttZeroPersistentHashMapNode<HashKey, int> originalFirst =
            originalRoot.Children[0];
        MqttZeroPersistentHashMapNode<HashKey, int> originalSecond =
            originalRoot.Children[1];

        MqttZeroPersistentHashMap<HashKey, int> updated =
            original.Set(firstKey, 11, out _);
        MqttZeroPersistentHashMapBitmapNode<HashKey, int> updatedRoot =
            Assert.IsType<MqttZeroPersistentHashMapBitmapNode<HashKey, int>>(
                updated.Root);

        Assert.NotSame(originalFirst, updatedRoot.Children[0]);
        Assert.Same(originalSecond, updatedRoot.Children[1]);
        Assert.Same(originalSecond, originalRoot.Children[1]);
    }

    [Fact]
    public void ReadsDoNotAllocateAfterWarmup()
    {
        MqttZeroPersistentHashMap<string, int> map =
            MqttZeroPersistentHashMap<string, int>.Create(
                StringComparer.Ordinal);
        for (int index = 0; index < 100; index++)
        {
            map = map.Set(
                index.ToString(CultureInfo.InvariantCulture),
                index,
                out _);
        }

        for (int index = 0; index < 10; index++)
        {
            Assert.True(map.TryGetValue("50", out _));
        }

        for (int index = 0; index < 1_000; index++)
        {
            _ = map.TryGetValue("50", out int warmupValue);
            Assert.Equal(50, warmupValue);
        }

        _ = MeasureLookups(map, out int warmupSum);
        long allocated = MeasureLookups(map, out int sum);

        Assert.Equal(50_000, warmupSum);
        Assert.Equal(50_000, sum);
        Assert.Equal(0, allocated);
    }

    [Fact]
    public void EnumerationDoesNotAllocateAfterWarmup()
    {
        MqttZeroPersistentHashMap<string, int> map =
            MqttZeroPersistentHashMap<string, int>.Create(
                StringComparer.Ordinal);
        for (int index = 0; index < 100; index++)
        {
            map = map.Set(
                index.ToString(CultureInfo.InvariantCulture),
                index,
                out _);
        }

        for (int index = 0; index < 10; index++)
        {
            Assert.Equal(100, CountEntries(map));
        }

        _ = MeasureEnumeration(map, out int warmupEntries);
        long allocated = MeasureEnumeration(map, out int entries);

        Assert.Equal(100_000, warmupEntries);
        Assert.Equal(100_000, entries);
        Assert.Equal(0, allocated);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static long MeasureLookups(
        MqttZeroPersistentHashMap<string, int> map,
        out int sum)
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        sum = 0;
        for (int index = 0; index < 1_000; index++)
        {
            _ = map.TryGetValue("50", out int value);
            sum += value;
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static long MeasureEnumeration(
        MqttZeroPersistentHashMap<string, int> map,
        out int entries)
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        entries = 0;
        for (int index = 0; index < 1_000; index++)
        {
            entries += CountEntries(map);
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static int CountEntries(
        MqttZeroPersistentHashMap<string, int> map)
    {
        int count = 0;
        foreach (MqttZeroPersistentHashMapEntry<string, int> entry in map)
        {
            count += entry.Value >= 0 ? 1 : 0;
        }

        return count;
    }

    private readonly record struct CollisionKey(int Value);

    private sealed class CollisionKeyComparer :
        IEqualityComparer<CollisionKey>
    {
        public static CollisionKeyComparer Instance { get; } = new();

        public bool Equals(CollisionKey x, CollisionKey y)
        {
            return x.Value == y.Value;
        }

        public int GetHashCode(CollisionKey obj)
        {
            return 1;
        }
    }

    private readonly record struct HashKey(int Hash, int Value);

    private sealed class HashKeyComparer :
        IEqualityComparer<HashKey>
    {
        public static HashKeyComparer Instance { get; } = new();

        public bool Equals(HashKey x, HashKey y)
        {
            return x.Value == y.Value;
        }

        public int GetHashCode(HashKey obj)
        {
            return obj.Hash;
        }
    }
}
