using MqttZero.Protocol;
using MqttZero.Server.Sessions;
using System.Buffers;

namespace MqttZero.Server.Routing;

/// <summary>
/// 一个目标分片内的投递批次。
/// </summary>
internal struct MqttZeroFanoutBatch
{
    private MqttZeroPlannedDelivery[] _deliveries;

    /// <summary>
    /// 初始化 MqttZeroFanoutBatch 的新实例。
    /// </summary>
    /// <param name="shardId">分片Id。</param>
    internal MqttZeroFanoutBatch(int shardId)
    {
        ShardId = shardId;
        Count = 0;
        _deliveries = ArrayPool<MqttZeroPlannedDelivery>.Shared.Rent(16);
    }

    /// <summary>
    /// 分片Id。
    /// </summary>
    public int ShardId { get; }

    /// <summary>
    /// 数量。
    /// </summary>
    public int Count { get; private set; }

    public MqttZeroPlannedDelivery this[int index]
    {
        get
        {
            return _deliveries[index];
        }
    }

    /// <summary>
    /// As跨度。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public ReadOnlySpan<MqttZeroPlannedDelivery> AsSpan()
    {
        return _deliveries.AsSpan(0, Count);
    }

    /// <summary>
    /// 添加。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="retainAsPublished">保留AsPublished。</param>
    /// <param name="identifierSource">标识符来源。</param>
    /// <returns>计算得到的数值。</returns>
    internal int Add(
        MqttZeroSessionState session,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retainAsPublished,
        MqttZeroRoutePlanBuffer identifierSource)
    {
        if (Count == _deliveries.Length)
        {
            Grow();
        }

        int index = Count;
        _deliveries[index] = new MqttZeroPlannedDelivery(
            session,
            qualityOfServiceLevel,
            retainAsPublished,
            identifierSource);
        Count++;
        return index;
    }

    /// <summary>
    /// 拆离Deliveries。
    /// </summary>
    /// <returns>对应的结果。</returns>
    internal MqttZeroPlannedDelivery[] DetachDeliveries()
    {
        MqttZeroPlannedDelivery[] deliveries = _deliveries;
        _deliveries = [];
        Count = 0;
        return deliveries;
    }

    /// <summary>
    /// 获取DeliveryReference。
    /// </summary>
    /// <param name="index">索引。</param>
    /// <returns>对应的结果。</returns>
    internal ref MqttZeroPlannedDelivery GetDeliveryReference(int index)
    {
        return ref _deliveries[index];
    }

    /// <summary>
    /// 获取保留字节Estimate。
    /// </summary>
    /// <returns>计算得到的数值。</returns>
    internal long GetRetainedByteEstimate()
    {
        return
            (long)_deliveries.Length *
            System.Runtime.CompilerServices.Unsafe.SizeOf<
                MqttZeroPlannedDelivery>();
    }

    /// <summary>
    /// 重置。
    /// </summary>
    internal void Reset()
    {
        Count = 0;
    }

    /// <summary>
    /// 执行 Grow 对应的处理。
    /// </summary>
    private void Grow()
    {
        MqttZeroPlannedDelivery[] replacement =
            ArrayPool<MqttZeroPlannedDelivery>.Shared.Rent(
                _deliveries.Length * 2);
        Array.Copy(_deliveries, replacement, Count);
        ArrayPool<MqttZeroPlannedDelivery>.Shared.Return(
            _deliveries,
            clearArray: true);
        _deliveries = replacement;
    }
}
