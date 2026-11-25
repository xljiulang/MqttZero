using MqttZero.Server.Sessions;

namespace MqttZero.Server.Egress;

internal sealed class MqttZeroEgressDataBatch
{
    public List<MqttZeroQueuedMessage> Messages { get; } = new(128);

    public long ByteCount { get; private set; }

    public int Count
    {
        get
        {
            return Messages.Count;
        }
    }

    public void Add(
        MqttZeroQueuedMessage message,
        long byteCount)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentOutOfRangeException.ThrowIfNegative(byteCount);
        Messages.Add(message);
        ByteCount += byteCount;
    }

    public void Reset()
    {
        Messages.Clear();
        ByteCount = 0;
    }
}
