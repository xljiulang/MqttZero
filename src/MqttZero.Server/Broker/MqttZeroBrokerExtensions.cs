using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Publishing;
using System.Buffers;
using System.IO.Pipelines;

namespace MqttZero.Server.Broker;

/// <summary>
/// 服务端发布便捷扩展。
/// </summary>
public static class MqttZeroBrokerExtensions
{
    /// <summary>
    /// Publishes a server-originated message from a payload sequence.
    /// </summary>
    /// <param name="broker">The MQTTZero broker.</param>
    /// <param name="topic">The MQTT topic name.</param>
    /// <param name="payload">The message payload.</param>
    /// <param name="qualityOfServiceLevel">The requested quality of service.</param>
    /// <param name="retain">Whether the message should be retained.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task representing the publish operation.</returns>
    public static ValueTask PublishAsync(
        this IMqttZeroBroker broker,
        string topic,
        ReadOnlySequence<byte> payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(broker);
        return broker.PublishAsync(
            new MqttZeroPublishMessage
            {
                Topic = topic,
                Payload = payload,
                QualityOfServiceLevel = qualityOfServiceLevel,
                Retain = retain
            },
            cancellationToken);
    }

    /// <summary>
    /// Publishes a server-originated message from a pipe payload factory.
    /// </summary>
    /// <param name="broker">The MQTTZero broker.</param>
    /// <param name="topic">The MQTT topic name.</param>
    /// <param name="payloadFactory">A callback that writes the payload.</param>
    /// <param name="qualityOfServiceLevel">The requested quality of service.</param>
    /// <param name="retain">Whether the message should be retained.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task representing the publish operation.</returns>
    public static async ValueTask PublishAsync(
        this IMqttZeroBroker broker,
        string topic,
        Func<PipeWriter, ValueTask> payloadFactory,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(broker);
        ArgumentNullException.ThrowIfNull(payloadFactory);

        await using MqttZeroPayloadOwner owner =
            await MqttZeroPayloadOwnerFactory.CreateMultipleSegmentAsync(
                WritePayloadAsync,
                cancellationToken).ConfigureAwait(false);

        await broker.PublishAsync(
            new MqttZeroPublishMessage
            {
                Topic = topic,
                Payload = owner.Payload,
                QualityOfServiceLevel = qualityOfServiceLevel,
                Retain = retain
            },
            cancellationToken).ConfigureAwait(false);

        async ValueTask WritePayloadAsync(PipeWriter writer)
        {
            MqttZeroLimitedPipeWriter limitedWriter = new(
                writer,
                broker.MaximumPacketSize);
            await payloadFactory(limitedWriter).ConfigureAwait(false);
            await limitedWriter.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Publishes a server-originated message from a stream.
    /// </summary>
    /// <param name="broker">The MQTTZero broker.</param>
    /// <param name="topic">The MQTT topic name.</param>
    /// <param name="payload">The payload stream. The caller retains ownership.</param>
    /// <param name="qualityOfServiceLevel">The requested quality of service.</param>
    /// <param name="retain">Whether the message should be retained.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task representing the publish operation.</returns>
    public static ValueTask PublishAsync(
        this IMqttZeroBroker broker,
        string topic,
        Stream payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(payload);
        return broker.PublishAsync(
            topic,
            WritePayloadAsync,
            qualityOfServiceLevel,
            retain,
            cancellationToken);

        async ValueTask WritePayloadAsync(PipeWriter writer)
        {
            await CopyToPipeAsync(
                payload,
                writer,
                cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 复制ToPipe。
    /// </summary>
    /// <param name="source">来源。</param>
    /// <param name="destination">对应的 destination。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private static async ValueTask CopyToPipeAsync(
        Stream source,
        PipeWriter destination,
        CancellationToken cancellationToken)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(16 * 1024);
        try
        {
            while (true)
            {
                int read = await source.ReadAsync(buffer, cancellationToken)
                    .ConfigureAwait(false);
                if (read == 0)
                {
                    return;
                }

                await destination.WriteAsync(buffer.AsMemory(0, read), cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
