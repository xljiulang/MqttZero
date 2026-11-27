using MqttZero.Protocol;

namespace MqttZero.Server.Broker;

/// <summary>
/// 单条 SUBSCRIBE 的评估结果。
/// </summary>
internal readonly struct MqttZeroSubscriptionDecision
{
    /// <summary>
    /// 初始化 MqttZeroSubscriptionDecision 的新实例。
    /// </summary>
    /// <param name="outcome">对应的 outcome。</param>
    /// <param name="reasonCode">原因代码。</param>
    /// <param name="existed">对应的 existed。</param>
    private MqttZeroSubscriptionDecision(
        MqttZeroSubscriptionOutcome outcome,
        MqttReasonCode reasonCode,
        bool existed)
    {
        Outcome = outcome;
        ReasonCode = reasonCode;
        Existed = existed;
    }

    /// <summary>
    /// Outcome 的值。
    /// </summary>
    public MqttZeroSubscriptionOutcome Outcome { get; }

    /// <summary>
    /// 原因代码。
    /// </summary>
    public MqttReasonCode ReasonCode { get; }

    /// <summary>
    /// Existed 的值。
    /// </summary>
    public bool Existed { get; }

    /// <summary>
    /// CloseConnection 的值。
    /// </summary>
    public bool CloseConnection
    {
        get
        {
            return Outcome == MqttZeroSubscriptionOutcome.CloseConnection;
        }
    }

    /// <summary>
    /// 执行 Subscribed 对应的处理。
    /// </summary>
    /// <param name="existed">对应的 existed。</param>
    /// <param name="reasonCode">原因代码。</param>
    /// <returns>对应的结果。</returns>
    public static MqttZeroSubscriptionDecision Subscribed(
        bool existed,
        MqttReasonCode reasonCode)
    {
        return new MqttZeroSubscriptionDecision(
            MqttZeroSubscriptionOutcome.Subscribed,
            reasonCode,
            existed);
    }

    /// <summary>
    /// 执行 Consumed 对应的处理。
    /// </summary>
    /// <param name="reasonCode">原因代码。</param>
    /// <returns>对应的结果。</returns>
    public static MqttZeroSubscriptionDecision Consumed(MqttReasonCode reasonCode)
    {
        return new MqttZeroSubscriptionDecision(
            MqttZeroSubscriptionOutcome.Consumed,
            reasonCode,
            false);
    }

    /// <summary>
    /// 执行 QuotaExceeded 对应的处理。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public static MqttZeroSubscriptionDecision QuotaExceeded()
    {
        return new MqttZeroSubscriptionDecision(
            MqttZeroSubscriptionOutcome.QuotaExceeded,
            MqttReasonCode.QuotaExceeded,
            false);
    }

    /// <summary>
    /// 执行 Closed 对应的处理。
    /// </summary>
    /// <param name="reasonCode">原因代码。</param>
    /// <returns>对应的结果。</returns>
    public static MqttZeroSubscriptionDecision Closed(MqttReasonCode reasonCode)
    {
        return new MqttZeroSubscriptionDecision(
            MqttZeroSubscriptionOutcome.CloseConnection,
            reasonCode,
            false);
    }
}
