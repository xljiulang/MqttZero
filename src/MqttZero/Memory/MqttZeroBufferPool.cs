using MqttZero.Protocol;
using System.Buffers;
using System.Collections.Concurrent;

namespace MqttZero.Memory;

internal sealed class MqttZeroBufferPool : IDisposable
{
    private const int BucketCount = 23;
    private const int MinimumBucketSize = 256;
    private const int MaximumCachedBuffersPerBucket = 1024;

    private static readonly MqttZeroBufferPool SharedPool = new(512L * 1024 * 1024);
    private static readonly int[] BucketSizes = CreateBucketSizes();

    private readonly ConcurrentQueue<byte[]>[] _buckets;
    private readonly int[] _cachedCounts;
    private readonly SemaphoreSlim _capacityReleased = new(0, int.MaxValue);
    private readonly long _maximumBytes;
    private readonly ArrayPool<byte> _source;
    private long _rentedBytes;
    private int _disposed;

    public MqttZeroBufferPool(long maximumBytes)
        : this(maximumBytes, ArrayPool<byte>.Shared)
    {
    }

    public MqttZeroBufferPool(long maximumBytes, ArrayPool<byte> source)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumBytes);
        _maximumBytes = maximumBytes;
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _buckets = new ConcurrentQueue<byte[]>[BucketCount];
        _cachedCounts = new int[BucketCount];
        for (int index = 0; index < _buckets.Length; index++)
        {
            _buckets[index] = new ConcurrentQueue<byte[]>();
        }
    }

    public static MqttZeroBufferPool Shared
    {
        get
        {
            return SharedPool;
        }
    }

    public long RentedBytes
    {
        get
        {
            return Volatile.Read(ref _rentedBytes);
        }
    }

    public byte[] Rent(int minimumLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(minimumLength);
        if (TryRent(minimumLength, out byte[]? buffer))
        {
            return buffer;
        }

        throw new MqttZeroBufferPoolExhaustedException();
    }

    public bool TryRent(int minimumLength, out byte[] buffer)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        ArgumentOutOfRangeException.ThrowIfNegative(minimumLength);

        int bucketIndex = GetBucketIndex(minimumLength);
        if (_buckets[bucketIndex].TryDequeue(out byte[]? cached))
        {
            Interlocked.Decrement(ref _cachedCounts[bucketIndex]);
            if (!TryReserve(cached.Length))
            {
                Interlocked.Increment(ref _cachedCounts[bucketIndex]);
                _buckets[bucketIndex].Enqueue(cached);
                buffer = [];
                return false;
            }

            buffer = cached;
            return true;
        }

        int bucketSize = BucketSizes[bucketIndex];
        if (!TryReserve(bucketSize))
        {
            buffer = [];
            return false;
        }

        try
        {
            buffer = _source.Rent(bucketSize);
            return true;
        }
        catch
        {
            Interlocked.Add(ref _rentedBytes, -bucketSize);
            throw;
        }
    }

    public void Return(byte[] buffer)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        if (buffer.Length == 0)
        {
            return;
        }

        Interlocked.Add(ref _rentedBytes, -buffer.Length);
        _capacityReleased.Release();

        int bucketIndex = GetBucketIndex(buffer.Length);
        if (Volatile.Read(ref _disposed) == 0 &&
            Interlocked.Increment(ref _cachedCounts[bucketIndex]) <=
                MaximumCachedBuffersPerBucket)
        {
            _buckets[bucketIndex].Enqueue(buffer);
            return;
        }

        Interlocked.Decrement(ref _cachedCounts[bucketIndex]);

        _source.Return(buffer);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        foreach (ConcurrentQueue<byte[]> bucket in _buckets)
        {
            while (bucket.TryDequeue(out byte[]? buffer))
            {
                _source.Return(buffer);
            }
        }

        _capacityReleased.Dispose();
    }

    public async ValueTask WaitForCapacityAsync(CancellationToken cancellationToken)
    {
        await _capacityReleased.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private static int[] CreateBucketSizes()
    {
        int[] sizes = new int[BucketCount];
        int size = MinimumBucketSize;
        for (int index = 0; index < sizes.Length; index++)
        {
            sizes[index] = size;
            if (index < sizes.Length - 1)
            {
                size = checked(size * 2);
            }
        }

        return sizes;
    }

    private static int GetBucketIndex(int length)
    {
        if (length <= MinimumBucketSize)
        {
            return 0;
        }

        int index = 32 - int.LeadingZeroCount(length - 1) - 8;
        return index >= BucketCount ? BucketCount - 1 : index;
    }

    private bool TryReserve(int byteCount)
    {
        while (true)
        {
            long current = Volatile.Read(ref _rentedBytes);
            if (byteCount > _maximumBytes - current)
            {
                return false;
            }

            if (Interlocked.CompareExchange(
                ref _rentedBytes,
                current + byteCount,
                current) == current)
            {
                return true;
            }
        }
    }
}
