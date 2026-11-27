namespace MqttZero.Server.Routing;

/// <summary>
/// 一条发布的路由计划。
/// </summary>
internal readonly struct MqttZeroRoutePlan
{
    private readonly MqttZeroFanoutBatch[] _batches;
    private readonly int _shardCount;

    /// <summary>
    /// 初始化 MqttZeroRoutePlan 的新实例。
    /// </summary>
    /// <param name="batches">对应的 batches。</param>
    /// <param name="shardCount">分片数量。</param>
    /// <param name="nonEmptyBatchCount">non空批次数量。</param>
    /// <param name="totalDeliveryCount">totalDelivery数量。</param>
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

    /// <summary>
    /// Batches 的值。
    /// </summary>
    public ReadOnlySpan<MqttZeroFanoutBatch> Batches
    {
        get
        {
            return _batches.AsSpan(0, _shardCount);
        }
    }

    /// <summary>
    /// Non空批次数量。
    /// </summary>
    public int NonEmptyBatchCount { get; }

    /// <summary>
    /// TotalDelivery数量。
    /// </summary>
    public int TotalDeliveryCount { get; }
}
