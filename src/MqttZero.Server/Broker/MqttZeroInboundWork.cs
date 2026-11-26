using MqttZero.Memory;
using MqttZero.Server.Clients;
using MqttZero.Server.Sessions;

namespace MqttZero.Server.Broker;

internal sealed class MqttZeroInboundWork
{
    private static readonly MqttZeroBoundedObjectPool<MqttZeroInboundWork>
        Pool = new(
            MqttZeroBoundedObjectPool<MqttZeroInboundWork>.
                DefaultCapacity);
    private Action? _completed;

    private MqttZeroInboundWork()
    {
    }

    internal static int PoolCountForTest
    {
        get
        {
            return Pool.Count;
        }
    }

    public MqttZeroMessageLease Lease { get; private set; } = null!;

    public MqttZeroSessionState Sender { get; private set; } = null!;

    public MqttZeroClientConnection? SenderConnection { get; private set; }

    public long MessageId { get; private set; }

    public long CreatedTimestamp { get; private set; }

    public static MqttZeroInboundWork Rent(
        MqttZeroMessageLease lease,
        MqttZeroSessionState sender,
        MqttZeroClientConnection? senderConnection,
        long messageId = 0,
        Action? completed = null)
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
        work.CreatedTimestamp = System.Diagnostics.Stopwatch.GetTimestamp();
        return work;
    }

    public void Release()
    {
        Action? completed = _completed;
        _completed = null;
        Lease.Release();
        Lease = null!;
        Sender = null!;
        SenderConnection = null;
        MessageId = 0;
        completed?.Invoke();
        _ = Pool.Return(this);
    }
}
