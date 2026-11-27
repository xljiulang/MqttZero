namespace MqttZero.BrokerBenchmarks;

/// <summary>
/// Collects the latest broker control probe line of one A/B round.
/// </summary>
internal sealed class BrokerControlProbeCollector
{
    private readonly object _gate = new();
    private BrokerControlProbeSnapshot? _latest;

    public BrokerControlProbeSnapshot? Latest
    {
        get
        {
            lock (_gate)
            {
                return _latest;
            }
        }
    }

    public void Observe(BrokerControlProbeSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        lock (_gate)
        {
            _latest = snapshot;
        }
    }
}
