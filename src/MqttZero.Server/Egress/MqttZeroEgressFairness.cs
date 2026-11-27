namespace MqttZero.Server.Egress;

/// <summary>
/// Bounds how many control frames one egress pass may write before data drains.
/// </summary>
internal static class MqttZeroEgressFairness
{
    /// <summary>
    /// The maximum number of control frames written between two data drains.
    /// </summary>
    public const int MaximumControlFramesPerPass = 64;

    /// <summary>
    /// Gets the number of control frames to take from the queue for one batch.
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
    /// Gets a value indicating whether data must drain after this control batch.
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
