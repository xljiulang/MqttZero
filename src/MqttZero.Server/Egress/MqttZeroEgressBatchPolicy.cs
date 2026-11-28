namespace MqttZero.Server.Egress;

/// <summary>
/// 根据首条消息的编码长度调整单连接出站批次上限。
/// </summary>
internal static class MqttZeroEgressBatchPolicy
{
    private const int LargeMessageThreshold = 32 * 1024;
    private const int LargeMessageBytes = 128 * 1024;
    private const int LargeMessageCount = 2;

    /// <summary>
    /// 判断编码长度是否属于大消息。
    /// </summary>
    /// <param name="encodedLength">编码长度。</param>
    /// <returns>大消息时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    internal static bool IsLarge(int encodedLength)
    {
        return encodedLength >= LargeMessageThreshold;
    }

    /// <summary>
    /// 解析本轮出站批次的字节数和消息数上限。
    /// </summary>
    /// <param name="encodedLength">首条消息的编码长度。</param>
    /// <param name="activeConnectionCount">当前活动连接数。</param>
    /// <param name="configuredBytes">调用方配置的字节上限。</param>
    /// <param name="configuredMessages">调用方配置的消息数上限。</param>
    /// <returns>实际使用的字节数和消息数上限。</returns>
    internal static (int Bytes, int Messages) Resolve(
        int encodedLength,
        int activeConnectionCount,
        int configuredBytes,
        int configuredMessages)
    {
        if (!IsLarge(encodedLength) ||
            activeConnectionCount is > 0 and <= 2)
        {
            return (configuredBytes, configuredMessages);
        }

        return (
            Math.Min(configuredBytes, LargeMessageBytes),
            Math.Min(configuredMessages, LargeMessageCount));
    }
}
