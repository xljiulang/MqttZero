using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Clients;
using MqttZero.Server.Sessions;

namespace MqttZero.Server.Broker;

internal sealed class MqttZeroAcknowledgementWork
{
    private static readonly MqttZeroBoundedObjectPool<
        MqttZeroAcknowledgementWork> Pool = new(
            MqttZeroBoundedObjectPool<MqttZeroAcknowledgementWork>.
                DefaultCapacity);
    private Action? _completed;

    private MqttZeroAcknowledgementWork()
    {
    }

    public MqttZeroAcknowledgementView Acknowledgement
    { get; private set; }

    public MqttZeroClientConnection Connection { get; private set; } = null!;

    public MqttZeroSessionState Session { get; private set; } = null!;

    public static MqttZeroAcknowledgementWork Rent(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroAcknowledgementView acknowledgement,
        Action completed)
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
        return work;
    }

    public void Release()
    {
        Action? completed = _completed;
        _completed = null;
        Session = null!;
        Connection = null!;
        Acknowledgement = default;
        completed?.Invoke();
        _ = Pool.Return(this);
    }
}
