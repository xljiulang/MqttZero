using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Clients;
using MqttZero.Server.Sessions;

namespace MqttZero.Server.Broker;

/// <summary>
/// 在会话分片外提交、携带确认报文的池化工作项。
/// </summary>
internal sealed class MqttZeroAcknowledgementWork
{
    private static readonly MqttZeroBoundedObjectPool<
        MqttZeroAcknowledgementWork> Pool = new(
            MqttZeroBoundedObjectPool<MqttZeroAcknowledgementWork>.
                DefaultCapacity);
    private Action<TaskCompletionSource>? _completed;

    /// <summary>
    /// 初始化 MqttZeroAcknowledgementWork 的新实例。
    /// </summary>
    private MqttZeroAcknowledgementWork()
    {
    }

    internal static int PoolCountForTest
    {
        get
        {
            return Pool.Count;
        }
    }

    public MqttZeroAcknowledgementView Acknowledgement
    { get; private set; }

    /// <summary>
    /// 连接。
    /// </summary>
    public MqttZeroClientConnection Connection { get; private set; } = null!;

    /// <summary>
    /// 会话。
    /// </summary>
    public MqttZeroSessionState Session { get; private set; } = null!;

    /// <summary>
    /// Completion 的值。
    /// </summary>
    public TaskCompletionSource? Completion { get; private set; }

    /// <summary>
    /// Enqueued时间戳时钟滴答。
    /// </summary>
    public long EnqueuedTimestampTicks { get; set; }

    /// <summary>
    /// 租用。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="connection">连接。</param>
    /// <param name="acknowledgement">确认。</param>
    /// <param name="completed">对应的 completed。</param>
    /// <param name="completion">对应的 completion。</param>
    /// <returns>对应的结果。</returns>
    public static MqttZeroAcknowledgementWork Rent(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroAcknowledgementView acknowledgement,
        Action<TaskCompletionSource> completed,
        TaskCompletionSource completion)
    {
        ArgumentNullException.ThrowIfNull(completed);
        if (!Pool.TryRent(out MqttZeroAcknowledgementWork? work) ||
            work is null)
        {
            work = new MqttZeroAcknowledgementWork();
        }

        work.Session = session;
        work.Connection = connection;
        work.Acknowledgement = acknowledgement;
        work._completed = completed;
        work.Completion = completion;
        return work;
    }

    /// <summary>
    /// 释放。
    /// </summary>
    public void Release()
    {
        Action<TaskCompletionSource>? completed = _completed;
        TaskCompletionSource? completion = Completion;
        _completed = null;
        Completion = null;
        Session = null!;
        Connection = null!;
        Acknowledgement = default;
        EnqueuedTimestampTicks = 0;
        if (completed != null && completion != null)
        {
            completed(completion);
        }

        _ = Pool.Return(this);
    }
}
