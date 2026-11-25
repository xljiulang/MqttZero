using BenchmarkDotNet.Attributes;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;

namespace MqttZero.Benchmarks;

/// <summary>
/// Measures regular and shared subscription planning across Shards.
/// </summary>
[MemoryDiagnoser]
public class RoutePlanningBenchmarks : IDisposable
{
    private static readonly byte[] Topic = "plan/topic"u8.ToArray();

    private MqttZeroSharedGroupCursors _cursors = null!;
    private MqttZeroRouteEpoch _epoch = null!;
    private MqttZeroRoutePlannerLanes _lanes = null!;

    /// <summary>
    /// Gets or sets the subscriber count.
    /// </summary>
    [Params(100, 1000)]
    public int Subscribers { get; set; }

    /// <summary>
    /// Gets or sets the target Shard count.
    /// </summary>
    [Params(1, 8)]
    public int Shards { get; set; }

    /// <summary>
    /// Gets or sets whether sessions use a shared subscription group.
    /// </summary>
    [Params(false, true)]
    public bool Shared { get; set; }

    /// <summary>
    /// Builds the epoch and per-Shard planner lanes.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        string filter = Shared ? "$share/workers/plan/#" : "plan/#";
        _epoch = MqttZeroRouteEpoch.Empty;
        for (int index = 0; index < Subscribers; index++)
        {
            _epoch = _epoch.Add(
                new MqttZeroSessionState(
                    $"planning-{index}",
                    100,
                    index % Shards),
                new MqttZeroSubscription(
                    System.Text.Encoding.UTF8.GetBytes(filter),
                    MqttQualityOfServiceLevel.AtMostOnce,
                    false,
                    false,
                    (uint)index + 1));
        }

        _cursors = new MqttZeroSharedGroupCursors();
        _lanes = new MqttZeroRoutePlannerLanes(Shards);
    }

    /// <summary>
    /// Plans one publish for the configured subscription topology.
    /// </summary>
    /// <returns>The planned delivery count.</returns>
    [Benchmark]
    public int Plan()
    {
        return MqttZeroRoutePlanner.Plan(
            _epoch,
            Topic,
            null,
            _cursors,
            Shards,
            _lanes[0]).TotalDeliveryCount;
    }

    /// <summary>
    /// Releases pooled planner state.
    /// </summary>
    public void Dispose()
    {
        _lanes.Dispose();
        GC.SuppressFinalize(this);
    }
}
