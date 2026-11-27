using MqttZero.Memory;
using MqttZero.Server.Clients;
using MqttZero.Server.Sessions;

namespace MqttZero.Server.Broker;

/// <summary>
/// 入站发布工作项。
/// </summary>
internal sealed class MqttZeroInboundWork
{
    private static readonly MqttZeroBoundedObjectPool<MqttZeroInboundWork>
        Pool = new(
            MqttZeroBoundedObjectPool<MqttZeroInboundWork>.
                DefaultCapacity);
    private Action<TaskCompletionSource>? _completed;

    /// <summary>
    /// 初始化 MqttZeroInboundWork 的新实例。
    /// </summary>
    private MqttZeroInboundWork()
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
    /// 租约。
    /// </summary>
    public MqttZeroMessageLease Lease { get; private set; } = null!;

    /// <summary>
    /// Sender 的值。
    /// </summary>
    public MqttZeroSessionState Sender { get; private set; } = null!;

    /// <summary>
    /// Sender连接。
    /// </summary>
    public MqttZeroClientConnection? SenderConnection { get; private set; }

    /// <summary>
    /// 消息Id。
    /// </summary>
    public long MessageId { get; private set; }

    /// <summary>
    /// Created时间戳。
    /// </summary>
    public long CreatedTimestamp { get; private set; }

    /// <summary>
    /// Completion 的值。
    /// </summary>
    public TaskCompletionSource? Completion { get; private set; }

    /// <summary>
    /// 租用。
    /// </summary>
    /// <param name="lease">租约。</param>
    /// <param name="sender">对应的 sender。</param>
    /// <param name="senderConnection">sender连接。</param>
    /// <param name="messageId">消息Id。</param>
    /// <param name="completed">对应的 completed。</param>
    /// <param name="completion">对应的 completion。</param>
    /// <returns>对应的结果。</returns>
    public static MqttZeroInboundWork Rent(
        MqttZeroMessageLease lease,
        MqttZeroSessionState sender,
        MqttZeroClientConnection? senderConnection,
        long messageId = 0,
        Action<TaskCompletionSource>? completed = null,
        TaskCompletionSource? completion = null)
    {
        if (!Pool.TryRent(out MqttZeroInboundWork? work) ||
            work is null)
        {
            work = new MqttZeroInboundWork();
        }

        lease.Retain();
        work.Lease = lease;
        work.Sender = sender;
        work.SenderConnection = senderConnection;
        work.MessageId = messageId;
        work._completed = completed;
        work.Completion = completion;
        work.CreatedTimestamp = System.Diagnostics.Stopwatch.GetTimestamp();
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
        Lease.Release();
        Lease = null!;
        Sender = null!;
        SenderConnection = null;
        MessageId = 0;
        if (completed != null && completion != null)
        {
            completed(completion);
        }

        _ = Pool.Return(this);
    }
}
