namespace MqttZero.Server.Routing;

internal readonly struct MqttZeroRoutePlan
{
    private readonly MqttZeroFanoutBatch[] _batches;
    private readonly int _shardCount;

    internal MqttZeroRoutePlan(
        MqttZeroFanoutBatch[] batches,
        int shardCount,
        int nonEmptyBatchCount,
        int totalDeliveryCount)
    {
        _batches = batches;
        _shardCount = shardCount;
        NonEmptyBatchCount = nonEmptyBatchCount;
        TotalDeliveryCount = totalDeliveryCount;
    }

    public ReadOnlySpan<MqttZeroFanoutBatch> Batches
    {
        get
        {
            return _batches.AsSpan(0, _shardCount);
        }
    }

    public int NonEmptyBatchCount { get; }

    public int TotalDeliveryCount { get; }
}
