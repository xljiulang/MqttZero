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
    private Action<TaskCompletionSource>? _completed;

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

    public MqttZeroClientConnection Connection { get; private set; } = null!;

    public MqttZeroSessionState Session { get; private set; } = null!;

    public TaskCompletionSource? Completion { get; private set; }

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

    public void Release()
    {
        Action<TaskCompletionSource>? completed = _completed;
        TaskCompletionSource? completion = Completion;
        _completed = null;
        Completion = null;
        Session = null!;
        Connection = null!;
        Acknowledgement = default;
        if (completed != null && completion != null)
        {
            completed(completion);
        }

        _ = Pool.Return(this);
    }
}
