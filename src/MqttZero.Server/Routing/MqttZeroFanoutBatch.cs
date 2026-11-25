using MqttZero.Protocol;
using MqttZero.Server.Sessions;
using System.Buffers;

namespace MqttZero.Server.Routing;

internal struct MqttZeroFanoutBatch
{
    private MqttZeroPlannedDelivery[] _deliveries;

    internal MqttZeroFanoutBatch(int shardId)
    {
        ShardId = shardId;
        Count = 0;
        _deliveries = ArrayPool<MqttZeroPlannedDelivery>.Shared.Rent(16);
    }

    public int ShardId { get; }

    public int Count { get; private set; }

    public MqttZeroPlannedDelivery this[int index]
    {
        get
        {
            return _deliveries[index];
        }
    }

    public ReadOnlySpan<MqttZeroPlannedDelivery> AsSpan()
    {
        return _deliveries.AsSpan(0, Count);
    }

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

    internal MqttZeroPlannedDelivery[] DetachDeliveries()
    {
        MqttZeroPlannedDelivery[] deliveries = _deliveries;
        _deliveries = [];
        Count = 0;
        return deliveries;
    }

    internal ref MqttZeroPlannedDelivery GetDeliveryReference(int index)
    {
        return ref _deliveries[index];
    }

    internal void Reset()
    {
        Count = 0;
    }

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
