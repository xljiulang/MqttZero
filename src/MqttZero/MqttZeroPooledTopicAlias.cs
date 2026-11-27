namespace MqttZero;

/// <summary>
/// 池化存储的主题别名。
/// </summary>
internal sealed class MqttZeroPooledTopicAlias
{
    /// <summary>
    /// 初始化 MqttZeroPooledTopicAlias 的新实例。
    /// </summary>
    /// <param name="buffer">缓冲区。</param>
    /// <param name="length">长度。</param>
    public MqttZeroPooledTopicAlias(byte[] buffer, int length)
    {
        Buffer = buffer;
        Length = length;
    }

    /// <summary>
    /// 缓冲区。
    /// </summary>
    public byte[] Buffer { get; }

    /// <summary>
    /// 长度。
    /// </summary>
    public int Length { get; set; }
}
