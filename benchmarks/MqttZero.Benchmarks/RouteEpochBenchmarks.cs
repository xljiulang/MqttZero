using BenchmarkDotNet.Attributes;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;

namespace MqttZero.Benchmarks;

/// <summary>
/// Measures immutable route epoch traversal for exact and wildcard filters.
/// </summary>
[MemoryDiagnoser]
public class RouteEpochBenchmarks : IDisposable
{
    private static readonly byte[] ExactTopic = "route/topic"u8.ToArray();

    private MqttZeroRouteEpoch _epoch = null!;
    private MqttZeroRoutePlannerLanes _lanes = null!;
    private MqttZeroSharedGroupCursors _cursors = null!;

    /// <summary>
    /// Gets or sets the subscriber count.
    /// </summary>
    [Params(1, 100, 1000)]
    public int Subscribers { get; set; }

    /// <summary>
    /// Gets or sets the filter operation.
    /// </summary>
    [Params("exact", "+", "#")]
    public string Operation { get; set; } = string.Empty;

    /// <summary>
    /// Builds the immutable epoch used by the benchmark.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        string filter = Operation switch
        {
            "exact" => "route/topic",
            "+" => "route/+",
            "#" => "route/#",
            _ => throw new InvalidOperationException(
                "The route epoch benchmark operation is invalid.")
        };
        _epoch = MqttZeroRouteEpoch.Empty;
        for (int index = 0; index < Subscribers; index++)
        {
            _epoch = _epoch.Add(
                new MqttZeroSessionState($"epoch-{index}", 100, 0),
                new MqttZeroSubscription(
                    System.Text.Encoding.UTF8.GetBytes(filter),
                    MqttQualityOfServiceLevel.AtMostOnce,
                    false,
                    false,
                    null));
        }

        _lanes = new MqttZeroRoutePlannerLanes(1);
        _cursors = new MqttZeroSharedGroupCursors();
    }

    /// <summary>
    /// Plans deliveries by traversing the current epoch.
    /// </summary>
    /// <returns>The planned delivery count.</returns>
    [Benchmark]
    public int Plan()
    {
        return MqttZeroRoutePlanner.Plan(
            _epoch,
            ExactTopic,
            null,
            _cursors,
            1,
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
