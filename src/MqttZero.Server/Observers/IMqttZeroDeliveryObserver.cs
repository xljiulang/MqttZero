namespace MqttZero.Server.Observers;

/// <summary>
/// 观察投递与队列结果。
/// </summary>
public interface IMqttZeroDeliveryObserver
{
    /// <summary>投递入队或转为在途后调用。</summary>
    ValueTask OnMessageEnqueuedAsync(MqttZeroMessageEnqueuedContext context, CancellationToken cancellationToken);
    /// <summary>投递被丢弃时调用。</summary>
    ValueTask OnMessageDroppedAsync(MqttZeroMessageDroppedContext context, CancellationToken cancellationToken);
    /// <summary>排队消息被覆盖前调用。</summary>
    ValueTask OnQueueMessageOverwrittenAsync(MqttZeroQueueMessageOverwrittenContext context, CancellationToken cancellationToken);
    /// <summary>没有订阅者消费该发布时调用。</summary>
    ValueTask OnMessageNotConsumedAsync(MqttZeroMessageNotConsumedContext context, CancellationToken cancellationToken);
}
