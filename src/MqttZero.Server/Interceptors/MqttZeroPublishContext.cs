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
    private static readonly MqttZeroBoundedObjectPool<MqttZeroPublishContext> ContextPool = new(MqttZeroBoundedObjectPool<MqttZeroPublishContext>.DefaultCapacity);

    /// <summary>
    /// 获取或设置 UTF-8 主题名。
    /// </summary>
    public ReadOnlySequence<byte> Topic { get; set; }

    /// <summary>
    /// 获取或设置负载序列。
    /// </summary>
    public ReadOnlySequence<byte> Payload { get; set; }

    /// <summary>
    /// 获取或设置随发布传递的 MQTT 5 属性。
    /// </summary>
    public MqttPublishProperties Properties { get; set; } = MqttPublishProperties.Empty;

    /// <summary>
    /// 获取或设置服务质量等级。
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; set; }

    /// <summary>
    /// 获取或设置消息是否作为保留消息发布。
    /// </summary>
    public bool Retain { get; set; }

    /// <summary>获取消息来源客户端；服务端发起的消息为 null。</summary>
    public MqttZeroConnectedClient? Sender { get; internal set; }

    /// <summary>获取或设置结果原因码。</summary>
    public MqttReasonCode ResultReasonCode { get; set; } = MqttReasonCode.Success;

    /// <summary>获取或设置可选的结果原因字符串。</summary>
    public string? ReasonString { get; set; }

    /// <summary>
    /// PoolCountForTest 的值。
    /// </summary>
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
