namespace MqttZero.Server.Scheduling;

/// <summary>
/// 分片工作项的基类。
/// </summary>
internal abstract class MqttZeroShardWork
{
    /// <summary>
    /// Enqueued时间戳。
    /// </summary>
    internal long EnqueuedTimestamp { get; private set; }

    public virtual bool IsLowPriority
    {
        get
        {
            return false;
        }
    }

    /// <summary>
    /// 执行。
    /// </summary>
    /// <param name="shard">分片。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>对应的结果。</returns>
    public abstract ValueTask ExecuteAsync(
        MqttZeroEventLoop shard,
        CancellationToken cancellationToken);

    /// <summary>
    /// 释放。
    /// </summary>
    public virtual void Release()
    {
    }

    /// <summary>
    /// 执行 MarkEnqueued 对应的处理。
    /// </summary>
    internal void MarkEnqueued()
    {
        EnqueuedTimestamp = System.Diagnostics.Stopwatch.GetTimestamp();
    }
}
