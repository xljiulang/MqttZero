using MqttZero.Server.Scheduling;

namespace MqttZero.Server.Egress;

/// <summary>
/// Transfers one encoded control frame to the egress shard without waiting for
/// the frame to be written.
/// </summary>
internal sealed class MqttZeroControlPostWork : MqttZeroShardWork
{
    private MqttZeroEgressControlFrame? _frame;
    private MqttZeroSessionEgress? _egress;

    private MqttZeroControlPostWork()
    {
    }

    internal static MqttZeroControlPostWork Rent(
        MqttZeroSessionEgress egress,
        MqttZeroEgressControlFrame frame)
    {
        return new MqttZeroControlPostWork
        {
            _egress = egress,
            _frame = frame
        };
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
        egress.AcceptControlFromShard(frame);
        return ValueTask.CompletedTask;
    }

    public override void Release()
    {
        _egress = null;
        _frame = null;
    }
}
