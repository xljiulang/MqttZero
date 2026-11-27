namespace MqttZero.Server.Egress;

/// <summary>
/// 每轮允许写出的控制帧上限。
/// </summary>
internal static class MqttZeroEgressFairness
{
    /// <summary>
    /// 两次数据排空之间允许写出的控制帧上限。
    /// </summary>
    public const int MaximumControlFramesPerPass = 64;

    /// <summary>
    /// 获取单批从队列取出的控制帧数量。
    /// </summary>
    /// <param name="queuedCount">The number of queued control frames.</param>
    /// <returns>The batch size, never above the per-pass budget.</returns>
    public static int GetControlBatchSize(int queuedCount)
    {
        return Math.Clamp(
            queuedCount,
            0,
            MaximumControlFramesPerPass);
    }

    /// <summary>
    /// 获取本控制批次之后是否必须排空数据。
    /// </summary>
    /// <param name="controlFramesSinceDataPass">
    /// The control frames written since the last data drain.
    /// </param>
    /// <param name="dataSignaled">Whether data is waiting.</param>
    /// <returns><see langword="true"/> when one data batch must run.</returns>
    public static bool ShouldDrainData(
        int controlFramesSinceDataPass,
        bool dataSignaled)
    {
        return dataSignaled &&
            controlFramesSinceDataPass >= MaximumControlFramesPerPass;
    }
}
