using System.Buffers;
using System.IO.Pipelines;

namespace MqttZero;

/// <summary>
/// 创建池化负载所有者的工厂。
/// </summary>
public static class MqttZeroPayloadOwnerFactory
{
    /// <summary>
    /// Creates a single-segment payload in pooled memory.
    /// </summary>
    /// <param name="payloadSize">The number of bytes made available to the callback.</param>
    /// <param name="payloadFactory">A callback that writes the payload.</param>
    /// <returns>A payload owner that must be disposed after use.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="payloadSize"/> is negative.</exception>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="payloadFactory"/> is null.</exception>
    public static MqttZeroPayloadOwner CreateSingleSegment(
        int payloadSize,
        Action<Memory<byte>> payloadFactory)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(payloadSize);
        ArgumentNullException.ThrowIfNull(payloadFactory);

        if (payloadSize == 0)
        {
            payloadFactory(Memory<byte>.Empty);
            return EmptyPayloadOwner.Instance;
        }

        byte[] buffer = ArrayPool<byte>.Shared.Rent(payloadSize);
        Memory<byte> payload = buffer.AsMemory(0, payloadSize);

        try
        {
            payloadFactory(payload);
            return new SingleSegmentPayloadOwner(buffer, payload);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(buffer);
            throw;
        }
    }

    /// <summary>
    /// Creates a potentially multi-segment payload using a recyclable pipe abstraction.
    /// </summary>
    /// <param name="payloadFactory">A callback that writes to the payload pipe.</param>
    /// <param name="cancellationToken">A token that can cancel payload construction.</param>
    /// <returns>A task that produces a payload owner.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="payloadFactory"/> is null.</exception>
    public static async ValueTask<MqttZeroPayloadOwner> CreateMultipleSegmentAsync(
        Func<PipeWriter, ValueTask> payloadFactory,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(payloadFactory);

        Pipe pipe = new(new PipeOptions(
            pool: MemoryPool<byte>.Shared,
            useSynchronizationContext: false,
            pauseWriterThreshold: -1,
            resumeWriterThreshold: -1));

        try
        {
            await payloadFactory(pipe.Writer).ConfigureAwait(false);
            await pipe.Writer.CompleteAsync().ConfigureAwait(false);

            ReadResult result = await pipe.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            while (!result.IsCompleted)
            {
                pipe.Reader.AdvanceTo(result.Buffer.Start, result.Buffer.Start);
                result = await pipe.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            }

            return new MultipleSegmentPayloadOwner(pipe, result.Buffer);
        }
        catch
        {
            await pipe.Reader.CompleteAsync().ConfigureAwait(false);
            await pipe.Writer.CompleteAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// 空负载持有者。
    /// </summary>
    private sealed class EmptyPayloadOwner : MqttZeroPayloadOwner
    {
        /// <summary>
        /// Instance 的值。
        /// </summary>
        public static EmptyPayloadOwner Instance { get; } = new();

        /// <summary>
        /// Payload 的值。
        /// </summary>
        public override ReadOnlySequence<byte> Payload
        {
            get
            {
                return ReadOnlySequence<byte>.Empty;
            }
        }
    }

    /// <summary>
    /// 单段负载持有者。
    /// </summary>
    private sealed class SingleSegmentPayloadOwner : MqttZeroPayloadOwner
    {
        private readonly byte[] _buffer;

        /// <summary>
        /// 初始化 SingleSegmentPayloadOwner 的新实例。
        /// </summary>
        /// <param name="buffer">缓冲区。</param>
        /// <param name="payload">负载。</param>
        public SingleSegmentPayloadOwner(byte[] buffer, Memory<byte> payload)
        {
            _buffer = buffer;
            Payload = new ReadOnlySequence<byte>(payload);
        }

        /// <summary>
        /// 负载。
        /// </summary>
        public override ReadOnlySequence<byte> Payload { get; }

        /// <summary>
        /// 释放。
        /// </summary>
        /// <returns>表示异步操作的任务。</returns>
        protected override ValueTask DisposeAsyncCore()
        {
            ArrayPool<byte>.Shared.Return(_buffer);
            return base.DisposeAsyncCore();
        }
    }

    /// <summary>
    /// 多段负载持有者。
    /// </summary>
    private sealed class MultipleSegmentPayloadOwner : MqttZeroPayloadOwner
    {
        private readonly Pipe _pipe;

        /// <summary>
        /// 初始化 MultipleSegmentPayloadOwner 的新实例。
        /// </summary>
        /// <param name="pipe">对应的 pipe。</param>
        /// <param name="payload">负载。</param>
        public MultipleSegmentPayloadOwner(Pipe pipe, ReadOnlySequence<byte> payload)
        {
            _pipe = pipe;
            Payload = payload;
        }

        /// <summary>
        /// 负载。
        /// </summary>
        public override ReadOnlySequence<byte> Payload { get; }

        /// <summary>
        /// 释放。
        /// </summary>
        /// <returns>表示异步操作的任务。</returns>
        protected override async ValueTask DisposeAsyncCore()
        {
            await _pipe.Reader.CompleteAsync().ConfigureAwait(false);
            await base.DisposeAsyncCore().ConfigureAwait(false);
        }
    }
}
