using MqttZero.Memory;
using MqttZero.Server.Scheduling;

namespace MqttZero.Server.Egress;

/// <summary>
/// Transfers one encoded control frame to the egress shard without waiting for
/// the frame to be written.
/// </summary>
internal sealed class MqttZeroControlPostWork : MqttZeroShardWork
{
    private static readonly MqttZeroBoundedObjectPool<MqttZeroControlPostWork>
        Pool = new(
            MqttZeroBoundedObjectPool<MqttZeroControlPostWork>.
                DefaultCapacity);
    private MqttZeroEgressControlFrame? _frame;
    private MqttZeroSessionEgress? _egress;

    private MqttZeroControlPostWork()
    {
    }

    internal static int PoolCountForTest
    {
        get
        {
            return Pool.Count;
        }
    }

    internal static MqttZeroControlPostWork Rent(
        MqttZeroSessionEgress egress,
        MqttZeroEgressControlFrame frame)
    {
        ArgumentNullException.ThrowIfNull(egress);
        ArgumentNullException.ThrowIfNull(frame);
        if (!Pool.TryRent(out MqttZeroControlPostWork? work) ||
            work is null)
        {
            work = new MqttZeroControlPostWork();
        }

        work._egress = egress;
        work._frame = frame;
        return work;
    }

    public override ValueTask ExecuteAsync(
        MqttZeroEventLoop shard,
        CancellationToken cancellationToken)
    {
        MqttZeroSessionEgress egress = _egress ??
            throw new InvalidOperationException(
                "The control post work was not initialized.");
        MqttZeroEgressControlFrame frame = _frame ??
            throw new InvalidOperationException(
                "The control post frame was not initialized.");
        _frame = null;
        egress.AcceptControlFromShard(frame);
        return ValueTask.CompletedTask;
    }

    public override void Release()
    {
        MqttZeroEgressControlFrame? frame = _frame;
        _frame = null;
        _egress = null;
        if (frame != null)
        {
            frame.Fail(new InvalidOperationException(
                "The control frame was never handed to the egress."));
            frame.Release();
        }

        _ = Pool.Return(this);
    }
}
