using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// 承载一次性 PUBLISH 报文的缓冲区。
/// </summary>
internal readonly struct MqttZeroPublishPacketBuffer
{
    /// <summary>
    /// 初始化 MqttZeroPublishPacketBuffer 的新实例。
    /// </summary>
    /// <param name="header">头部。</param>
    /// <param name="payload">负载。</param>
    public MqttZeroPublishPacketBuffer(
        ReadOnlyMemory<byte> header,
        ReadOnlySequence<byte> payload)
    {
        Header = header;
        Payload = payload;
    }

    /// <summary>
    /// 头部。
    /// </summary>
    public ReadOnlyMemory<byte> Header { get; }

    /// <summary>
    /// 负载。
    /// </summary>
    public ReadOnlySequence<byte> Payload { get; }

    /// <summary>
    /// Length 的值。
    /// </summary>
    public int Length
    {
        get
        {
            return Header.Length + (int)Payload.Length;
        }
    }
}
