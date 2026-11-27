namespace MqttZero.Server.Broker;

internal sealed record MqttZeroObjectPoolSnapshot(
    int LeaseCount,
    int QueuedMessageCount,
    int PublishContextCount,
    int InboundWorkCount,
    int InboundShardWorkCount,
    int FanoutOperationCount,
    int FanoutBatchWorkCount,
    int FanoutScheduledBatchCount,
    int AcknowledgementWorkCount,
    int AcknowledgementShardWorkCount);
