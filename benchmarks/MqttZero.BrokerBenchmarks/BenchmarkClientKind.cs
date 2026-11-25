namespace MqttZero.BrokerBenchmarks;

/// <summary>
/// Identifies the MQTT client used by a benchmark comparison.
/// </summary>
public enum BenchmarkClientKind
{
    /// <summary>Uses the MQTTnet client.</summary>
    MqttNet,

    /// <summary>Uses the benchmark-only raw MQTT client.</summary>
    Raw
}
