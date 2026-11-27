namespace MqttZero.Protocol;

/// <summary>
/// PUBLISH 或 Will 负载的编码格式。
/// </summary>
public enum MqttPayloadFormatIndicator : byte
{
    /// <summary>The payload uses an unspecified byte format.</summary>
    UnspecifiedUtf8 = 0,

    /// <summary>The payload contains well-formed UTF-8 data.</summary>
    Utf8 = 1
}
