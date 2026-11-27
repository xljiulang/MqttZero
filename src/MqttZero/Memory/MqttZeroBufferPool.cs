using MqttZero.Protocol;
using System.Buffers;
using System.Collections.Concurrent;

namespace MqttZero.Memory;

/// <summary>
/// 受字节预算约束、按桶分级的缓冲区池。
/// </summary>
internal sealed class MqttZeroBufferPool : IDisposable
{
    private const int BucketCount = 23;
    private const int MinimumBucketSize = 256;
    private const int MaximumCachedBuffersPerBucket = 1024;

    private static readonly MqttZeroBufferPool SharedPool = new(512L * 1024 * 1024);
    private static readonly int[] BucketSizes = CreateBucketSizes();

    private readonly ConcurrentQueue<byte[]>[] _buckets;
    private readonly long[] _cachedBucketBytes;
    private readonly int[] _cachedCounts;
    private readonly SemaphoreSlim _capacityReleased = new(0, int.MaxValue);
    private readonly long _maximumBytes;
    private readonly ArrayPool<byte> _source;
    private int _capacityWaiters;
    private long _committedBytes;
    private long _rentedBytes;
    private long _returnGeneration;
    private int _disposed;

    /// <summary>
    /// 初始化 MqttZeroBufferPool 的新实例。
    /// </summary>
    /// <param name="maximumBytes">最大字节数。</param>
    public MqttZeroBufferPool(long maximumBytes)
        : this(maximumBytes, MqttZeroNoCacheArrayPool.Instance)
    {
    }

    /// <summary>
    /// 初始化 MqttZeroBufferPool 的新实例。
    /// </summary>
    /// <param name="maximumBytes">最大字节数。</param>
    /// <param name="source">来源。</param>
    public MqttZeroBufferPool(long maximumBytes, ArrayPool<byte> source)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumBytes);
        _maximumBytes = maximumBytes;
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _buckets = new ConcurrentQueue<byte[]>[BucketCount];
        _cachedBucketBytes = new long[BucketCount];
        _cachedCounts = new int[BucketCount];
        for (int index = 0; index < _buckets.Length; index++)
        {
            _buckets[index] = new ConcurrentQueue<byte[]>();
        }
    }

    /// <summary>
    /// Shared 的值。
    /// </summary>
    public static MqttZeroBufferPool Shared
    {
        get
        {
            return SharedPool;
        }
    }

    /// <summary>
    /// RentedBytes 的值。
    /// </summary>
    public long RentedBytes
    {
        get
        {
            return Volatile.Read(ref _rentedBytes);
        }
    }

    /// <summary>
    /// 租用。
    /// </summary>
    /// <param name="minimumLength">最小长度。</param>
    /// <returns>字节数组。</returns>
    public byte[] Rent(int minimumLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(minimumLength);
        if (TryRent(minimumLength, out byte[]? buffer))
        {
            return buffer;
        }

        throw new MqttZeroBufferPoolExhaustedException(
            BucketSizes[GetBucketIndex(minimumLength)],
            Volatile.Read(ref _returnGeneration));
    }

    /// <summary>
    /// 获取快照。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public MqttZeroBufferPoolSnapshot GetSnapshot()
    {
        MqttZeroBufferBucketSnapshot[] buckets =
            new MqttZeroBufferBucketSnapshot[BucketCount];
        for (int index = 0; index < buckets.Length; index++)
        {
            buckets[index] = new MqttZeroBufferBucketSnapshot(
                BucketSizes[index],
                Volatile.Read(ref _cachedCounts[index]),
                Volatile.Read(ref _cachedBucketBytes[index]));
        }

        long cachedBytes = 0;
        for (int index = 0; index < buckets.Length; index++)
        {
            cachedBytes += buckets[index].CachedBytes;
        }

        long rentedBytes = Volatile.Read(ref _rentedBytes);
        return new MqttZeroBufferPoolSnapshot(
            rentedBytes,
            cachedBytes,
            Volatile.Read(ref _committedBytes),
            buckets);
    }

    /// <summary>
    /// 尝试租用。
    /// </summary>
    /// <param name="minimumLength">最小长度。</param>
    /// <param name="buffer">缓冲区。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public bool TryRent(int minimumLength, out byte[] buffer)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        ArgumentOutOfRangeException.ThrowIfNegative(minimumLength);

        if (TryRentCached(minimumLength, out buffer))
        {
            return true;
        }

        int bucketIndex = GetBucketIndex(minimumLength);
        int bucketSize = BucketSizes[bucketIndex];
        if (!TryReserveWithReclaim(bucketIndex, bucketSize))
        {
            buffer = [];
            return false;
        }

        try
        {
            buffer = _source.Rent(bucketSize);
            int additionalBytes = buffer.Length - bucketSize;
            if (additionalBytes > 0 &&
                !TryReserveCommitted(additionalBytes))
            {
                _source.Return(buffer);
                Interlocked.Add(ref _committedBytes, -bucketSize);
                buffer = [];
                return false;
            }

            Interlocked.Add(ref _rentedBytes, buffer.Length);
            return true;
        }
        catch
        {
            Interlocked.Add(ref _committedBytes, -bucketSize);
            throw;
        }
    }

    /// <summary>
    /// 归还。
    /// </summary>
    /// <param name="buffer">缓冲区。</param>
    public void Return(byte[] buffer)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        if (buffer.Length == 0)
        {
            return;
        }

        Interlocked.Add(ref _rentedBytes, -buffer.Length);

        int bucketIndex = GetBucketIndex(buffer.Length);
        if (Volatile.Read(ref _disposed) == 0 &&
            Interlocked.Increment(ref _cachedCounts[bucketIndex]) <=
                MaximumCachedBuffersPerBucket)
        {
            Interlocked.Add(
                ref _cachedBucketBytes[bucketIndex],
                buffer.Length);
            _buckets[bucketIndex].Enqueue(buffer);
            SignalCapacityRelease();
            return;
        }

        Interlocked.Decrement(ref _cachedCounts[bucketIndex]);
        Interlocked.Add(ref _committedBytes, -buffer.Length);

        _source.Return(buffer);
        SignalCapacityRelease();
    }

    /// <summary>
    /// 释放。
    /// </summary>
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

        Array.Clear(_cachedCounts);
        Array.Clear(_cachedBucketBytes);
        Volatile.Write(ref _committedBytes, 0);
        _capacityReleased.Dispose();
    }

    /// <summary>
    /// 等待容量。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask WaitForCapacityAsync(CancellationToken cancellationToken)
    {
        return WaitForCapacityAsync(
            Volatile.Read(ref _returnGeneration),
            cancellationToken);
    }

    /// <summary>
    /// 等待容量。
    /// </summary>
    /// <param name="observedReturnGeneration">observed归还代。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public async ValueTask WaitForCapacityAsync(
        long observedReturnGeneration,
        CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _capacityWaiters);
        try
        {
            if (Volatile.Read(ref _returnGeneration) !=
                observedReturnGeneration)
            {
                return;
            }

            while (_capacityReleased.Wait(0, CancellationToken.None))
            {
            }

            long generation = Volatile.Read(ref _returnGeneration);
            while (Volatile.Read(ref _returnGeneration) == generation)
            {
                await _capacityReleased
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        finally
        {
            Interlocked.Decrement(ref _capacityWaiters);
        }
    }

    /// <summary>
    /// 创建BucketSizes。
    /// </summary>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// 获取Bucket索引。
    /// </summary>
    /// <param name="length">长度。</param>
    /// <returns>计算得到的数值。</returns>
    private static int GetBucketIndex(int length)
    {
        if (length <= MinimumBucketSize)
        {
            return 0;
        }

        int index = 32 - int.LeadingZeroCount(length - 1) - 8;
        return index >= BucketCount ? BucketCount - 1 : index;
    }

    /// <summary>
    /// 通知容量Release。
    /// </summary>
    private void SignalCapacityRelease()
    {
        Interlocked.Increment(ref _returnGeneration);
        if (Volatile.Read(ref _capacityWaiters) > 0)
        {
            _capacityReleased.Release();
        }
    }

    /// <summary>
    /// 尝试租用Cached。
    /// </summary>
    /// <param name="minimumLength">最小长度。</param>
    /// <param name="buffer">缓冲区。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private bool TryRentCached(
        int minimumLength,
        out byte[] buffer)
    {
        int startIndex = GetBucketIndex(minimumLength);
        for (int index = startIndex; index < BucketCount; index++)
        {
            while (_buckets[index].TryDequeue(out byte[]? cached))
            {
                Interlocked.Decrement(ref _cachedCounts[index]);
                Interlocked.Add(
                    ref _cachedBucketBytes[index],
                    -cached.Length);
                if (cached.Length < minimumLength)
                {
                    Interlocked.Increment(ref _cachedCounts[index]);
                    Interlocked.Add(
                        ref _cachedBucketBytes[index],
                        cached.Length);
                    _buckets[index].Enqueue(cached);
                    continue;
                }

                Interlocked.Add(ref _rentedBytes, cached.Length);
                buffer = cached;
                return true;
            }
        }

        buffer = [];
        return false;
    }

    /// <summary>
    /// 尝试ReserveCommitted。
    /// </summary>
    /// <param name="byteCount">字节数量。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private bool TryReserveCommitted(int byteCount)
    {
        while (true)
        {
            long current = Volatile.Read(ref _committedBytes);
            if (byteCount > _maximumBytes - current)
            {
                return false;
            }

            if (Interlocked.CompareExchange(
                ref _committedBytes,
                current + byteCount,
                current) == current)
            {
                return true;
            }
        }
    }

    /// <summary>
    /// 尝试ReserveWithReclaim。
    /// </summary>
    /// <param name="requestedBucketIndex">requestedBucket索引。</param>
    /// <param name="bucketSize">bucket大小。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private bool TryReserveWithReclaim(
        int requestedBucketIndex,
        int bucketSize)
    {
        if (TryReserveCommitted(bucketSize))
        {
            return true;
        }

        if (!TryReclaimCachedBuffers(requestedBucketIndex, bucketSize))
        {
            return false;
        }

        return TryReserveCommitted(bucketSize);
    }

    /// <summary>
    /// 尝试ReclaimCached缓冲区。
    /// </summary>
    /// <param name="requestedBucketIndex">requestedBucket索引。</param>
    /// <param name="bucketSize">bucket大小。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private bool TryReclaimCachedBuffers(
        int requestedBucketIndex,
        int bucketSize)
    {
        long committed = Volatile.Read(ref _committedBytes);
        long required = bucketSize - Math.Max(0, _maximumBytes - committed);
        if (required <= 0)
        {
            return true;
        }

        long released = 0;
        for (int index = BucketCount - 1;
            index >= 0 && released < required;
            index--)
        {
            if (index == requestedBucketIndex)
            {
                continue;
            }

            while (released < required &&
                _buckets[index].TryDequeue(out byte[]? cached))
            {
                Interlocked.Decrement(ref _cachedCounts[index]);
                Interlocked.Add(
                    ref _cachedBucketBytes[index],
                    -cached.Length);
                Interlocked.Add(ref _committedBytes, -cached.Length);
                _source.Return(cached);
                released += cached.Length;
            }
        }

        return released >= required;
    }
}
