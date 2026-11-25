namespace MqttZero.BrokerBenchmarks;

internal sealed record BenchmarkEnvironmentInfo(
    string OsDescription,
    string OsVersion,
    string RuntimeVersion,
    string ProcessArchitecture,
    string RuntimeIdentifier,
    int ProcessorCount,
    BenchmarkRuntimeSnapshot Runtime,
    BenchmarkTopologyInfo Topology,
    BenchmarkEgressOptions Egress);
