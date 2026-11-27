namespace MqttZero.Protocol;

/// <summary>
/// PUBLISH 或 Will 负载的编码格式。
/// </summary>
public enum MqttPayloadFormatIndicator : byte
{
    /// <summary>负载使用未指定的字节格式。</summary>
    UnspecifiedUtf8 = 0,

    /// <summary>负载为合法的 UTF-8 数据。</summary>
    Utf8 = 1
}
