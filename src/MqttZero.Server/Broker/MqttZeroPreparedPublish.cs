using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Interceptors;

namespace MqttZero.Server.Broker;

/// <summary>
/// 通过拦截器后等待分发的发布。
/// </summary>
internal readonly struct MqttZeroPreparedPublish
{
    /// <summary>
    /// 初始化 MqttZeroPreparedPublish 的新实例。
    /// </summary>
    /// <param name="lease">租约。</param>
    /// <param name="result">结果。</param>
    /// <param name="reasonCode">原因代码。</param>
    /// <param name="usesSourceLease">uses来源租约。</param>
    public MqttZeroPreparedPublish(
        MqttZeroMessageLease? lease,
        MqttZeroInterceptionResult result,
        MqttReasonCode reasonCode,
        bool usesSourceLease)
    {
        Lease = lease;
        Result = result;
        ReasonCode = reasonCode;
        UsesSourceLease = usesSourceLease;
    }

    /// <summary>
    /// 租约。
    /// </summary>
    public MqttZeroMessageLease? Lease { get; }

    /// <summary>
    /// 结果。
    /// </summary>
    public MqttZeroInterceptionResult Result { get; }

    /// <summary>
    /// 原因代码。
    /// </summary>
    public MqttReasonCode ReasonCode { get; }

    /// <summary>
    /// Uses来源租约。
    /// </summary>
    public bool UsesSourceLease { get; }
}
