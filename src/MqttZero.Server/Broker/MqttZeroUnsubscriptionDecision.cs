using MqttZero.Protocol;

namespace MqttZero.Server.Broker;

/// <summary>
/// 单条 UNSUBSCRIBE 的评估结果。
/// </summary>
internal readonly struct MqttZeroUnsubscriptionDecision
{
    /// <summary>
    /// 初始化 MqttZeroUnsubscriptionDecision 的新实例。
    /// </summary>
    /// <param name="reasonCode">原因代码。</param>
    /// <param name="changed">对应的 changed。</param>
    /// <param name="closeConnection">close连接。</param>
    private MqttZeroUnsubscriptionDecision(
        MqttReasonCode reasonCode,
        bool changed,
        bool closeConnection)
    {
        ReasonCode = reasonCode;
        Changed = changed;
        CloseConnection = closeConnection;
    }

    /// <summary>
    /// 原因代码。
    /// </summary>
    public MqttReasonCode ReasonCode { get; }

    /// <summary>
    /// Changed 的值。
    /// </summary>
    public bool Changed { get; }

    /// <summary>
    /// 关闭连接。
    /// </summary>
    public bool CloseConnection { get; }

    /// <summary>
    /// 执行 Unsubscribed 对应的处理。
    /// </summary>
    /// <param name="changed">对应的 changed。</param>
    /// <returns>对应的结果。</returns>
    public static MqttZeroUnsubscriptionDecision Unsubscribed(bool changed)
    {
        return new MqttZeroUnsubscriptionDecision(
            changed
                ? MqttReasonCode.Success
                : MqttReasonCode.NoSubscriptionExisted,
            changed,
            false);
    }

    /// <summary>
    /// 执行 Consumed 对应的处理。
    /// </summary>
    /// <param name="reasonCode">原因代码。</param>
    /// <returns>对应的结果。</returns>
    public static MqttZeroUnsubscriptionDecision Consumed(MqttReasonCode reasonCode)
    {
        return new MqttZeroUnsubscriptionDecision(reasonCode, false, false);
    }

    /// <summary>
    /// 执行 Closed 对应的处理。
    /// </summary>
    /// <param name="reasonCode">原因代码。</param>
    /// <returns>对应的结果。</returns>
    public static MqttZeroUnsubscriptionDecision Closed(MqttReasonCode reasonCode)
    {
        return new MqttZeroUnsubscriptionDecision(reasonCode, false, true);
    }
}
