using MqttZero.Memory;
using MqttZero.Server.Clients;
using MqttZero.Server.Sessions;
using System.Collections.Concurrent;

namespace MqttZero.Server.Broker;

internal sealed class MqttZeroInboundWork
{
    private static readonly ConcurrentQueue<MqttZeroInboundWork> Pool = new();

    private MqttZeroInboundWork()
    {
    }

    public MqttZeroMessageLease Lease { get; private set; } = null!;

    public MqttZeroSessionState Sender { get; private set; } = null!;

    public MqttZeroClientConnection SenderConnection { get; private set; } = null!;

    public long CreatedTimestamp { get; private set; }

    public static MqttZeroInboundWork Rent(
        MqttZeroMessageLease lease,
        MqttZeroSessionState sender,
        MqttZeroClientConnection senderConnection)
    {
        if (!Pool.TryDequeue(out MqttZeroInboundWork? work))
        {
            work = new MqttZeroInboundWork();
        }

        work.Lease = lease;
        work.Sender = sender;
        work.SenderConnection = senderConnection;
        work.CreatedTimestamp = System.Diagnostics.Stopwatch.GetTimestamp();
        return work;
    }

    public void Release()
    {
        Lease.Release();
        Lease = null!;
        Sender = null!;
        SenderConnection = null!;
        Pool.Enqueue(this);
    }
}
