using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Clients;
using System.Buffers;

namespace MqttZero.Server.Interceptors;

/// <summary>
/// 发布拦截器的可变上下文。
/// </summary>
public sealed class MqttZeroPublishContext
{
    private static readonly MqttZeroBoundedObjectPool<MqttZeroPublishContext>
        ContextPool = new(
            MqttZeroBoundedObjectPool<MqttZeroPublishContext>.
                DefaultCapacity);

    /// <summary>
    /// Gets or sets the UTF-8 topic name.
    /// </summary>
    public ReadOnlySequence<byte> Topic { get; set; }

    /// <summary>
    /// Gets or sets the payload sequence.
    /// </summary>
    public ReadOnlySequence<byte> Payload { get; set; }

    /// <summary>
    /// Gets or sets the end-to-end MQTT 5 publish properties.
    /// </summary>
    public MqttPublishProperties Properties { get; set; } = MqttPublishProperties.Empty;

    /// <summary>
    /// Gets or sets the quality of service.
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the message is retained.
    /// </summary>
    public bool Retain { get; set; }

    /// <summary>Gets the originating connected client, or null for server-originated messages.</summary>
    public MqttZeroConnectedClient? Sender { get; internal set; }

    /// <summary>Gets or sets the result reason code.</summary>
    public MqttReasonCode ResultReasonCode { get; set; } = MqttReasonCode.Success;

    /// <summary>Gets or sets an optional result reason string.</summary>
    public string? ReasonString { get; set; }

    internal static int PoolCountForTest
    {
        get
        {
            return ContextPool.Count;
        }
    }

    /// <summary>
    /// 租用。
    /// </summary>
    /// <returns>对应的结果。</returns>
    internal static MqttZeroPublishContext Rent()
    {
        if (ContextPool.TryRent(out MqttZeroPublishContext? context) &&
            context is not null)
        {
            return context;
        }

        return new MqttZeroPublishContext();
    }

    /// <summary>
    /// 释放。
    /// </summary>
    internal void Release()
    {
        Topic = ReadOnlySequence<byte>.Empty;
        Payload = ReadOnlySequence<byte>.Empty;
        Properties = MqttPublishProperties.Empty;
        QualityOfServiceLevel = MqttQualityOfServiceLevel.AtMostOnce;
        Retain = false;
        Sender = null;
        ResultReasonCode = MqttReasonCode.Success;
        ReasonString = null;
        _ = ContextPool.Return(this);
    }
}
