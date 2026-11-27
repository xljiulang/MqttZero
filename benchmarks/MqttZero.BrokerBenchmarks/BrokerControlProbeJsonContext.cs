using System.Text.Json.Serialization;

namespace MqttZero.BrokerBenchmarks;

[JsonSourceGenerationOptions(WriteIndented = false)]
[JsonSerializable(typeof(BrokerControlProbeSnapshot))]
internal sealed partial class BrokerControlProbeJsonContext :
    JsonSerializerContext
{
}
