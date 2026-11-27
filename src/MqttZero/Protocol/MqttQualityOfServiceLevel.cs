namespace MqttZero.Protocol;

/// <summary>
/// MQTT 服务质量等级。
/// </summary>
public enum MqttQualityOfServiceLevel : byte
{
    /// <summary>最多一次投递。</summary>
    AtMostOnce = 0,

    /// <summary>至少一次投递。</summary>
    AtLeastOnce = 1,

    /// <summary>恰好一次投递。</summary>
    ExactlyOnce = 2
}
