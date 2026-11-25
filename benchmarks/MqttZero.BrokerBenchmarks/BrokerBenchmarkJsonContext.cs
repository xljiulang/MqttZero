using System.Text.Json.Serialization;

namespace MqttZero.BrokerBenchmarks;

[JsonSourceGenerationOptions(WriteIndented = true)]
[JsonSerializable(typeof(BenchmarkReport))]
[JsonSerializable(typeof(BrokerBenchmarkResult))]
[JsonSerializable(typeof(OutboundBenchmarkResult))]
internal sealed partial class BrokerBenchmarkJsonContext :
    JsonSerializerContext
{
}
