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
    private static readonly MqttZeroBoundedObjectPool<MqttZeroAcknowledgementWork> Pool = new(MqttZeroBoundedObjectPool<MqttZeroAcknowledgementWork>.DefaultCapacity);
    private Action<TaskCompletionSource>? _completed;
    private MqttZeroClientConnection? _connection;
    private MqttZeroSessionState? _session;

    /// <summary>
    /// 初始化 MqttZeroAcknowledgementWork 的新实例。
    /// </summary>
    private MqttZeroAcknowledgementWork()
    {
    }

    /// <summary>
    /// PoolCountForTest 的值。
    /// </summary>
    internal static int PoolCountForTest
    {
        get
        {
            return Pool.Count;
        }
    }

    /// <summary>
    /// Acknowledgement 的值。
    /// </summary>
    public MqttZeroAcknowledgementView Acknowledgement { get; private set; }

    /// <summary>
    /// 获取处理该确认的连接；尚未租用时访问会抛出异常。
    /// </summary>
    public MqttZeroClientConnection Connection
    {
        get
        {
            return _connection ?? throw new InvalidOperationException(
                "确认工作项的连接尚未初始化。");
        }
    }

    /// <summary>
    /// 获取处理该确认的会话；尚未租用时访问会抛出异常。
    /// </summary>
    public MqttZeroSessionState Session
    {
        get
        {
            return _session ?? throw new InvalidOperationException(
                "确认工作项的会话尚未初始化。");
        }
    }

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

        work._session = session;
        work._connection = connection;
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
        _session = null;
        _connection = null;
        Acknowledgement = default;
        EnqueuedTimestampTicks = 0;
        if (completed != null && completion != null)
        {
            completed(completion);
        }

        _ = Pool.Return(this);
    }
}
