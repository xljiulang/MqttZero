using MqttZero.Protocol;
namespace MqttZero.Memory;

/// <summary>
/// 预算内无法满足租用时抛出的异常。
/// </summary>
internal sealed class MqttZeroBufferPoolExhaustedException : InvalidOperationException
{
    /// <summary>
    /// 初始化 MqttZeroBufferPoolExhaustedException 的新实例。
    /// </summary>
    public MqttZeroBufferPoolExhaustedException()
        : this(0, 0)
    {
    }

    /// <summary>
    /// 初始化 MqttZeroBufferPoolExhaustedException 的新实例。
    /// </summary>
    /// <param name="requiredBytes">必需字节数。</param>
    /// <param name="returnGeneration">归还代。</param>
    public MqttZeroBufferPoolExhaustedException(
        int requiredBytes,
        long returnGeneration)
        : base("The MQTTZero buffer pool byte limit was reached.")
    {
        RequiredBytes = requiredBytes;
        ReturnGeneration = returnGeneration;
    }

    /// <summary>
    /// 必需字节数。
    /// </summary>
    public int RequiredBytes { get; }

    /// <summary>
    /// 归还代。
    /// </summary>
    public long ReturnGeneration { get; }
}
