using BenchmarkDotNet.Attributes;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;

namespace MqttZero.Benchmarks;

/// <summary>
/// Measures shared-subscription group selection.
/// </summary>
[MemoryDiagnoser]
public class SharedRoutingBenchmarks : IDisposable
{
    private static readonly byte[] Topic = "route/topic"u8.ToArray();

    private readonly MqttZeroRouteScopePool _scopePool = new();

    private MqttZeroSubscriptionRouteSnapshot _snapshot = null!;

    /// <summary>
    /// Gets or sets the subscriber count.
    /// </summary>
    [Params(1, 100, 1000)]
    public int Subscribers { get; set; }

    /// <summary>
    /// Creates one shared group with independent sessions.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        MqttZeroSharedRouteEntry[] entries =
            new MqttZeroSharedRouteEntry[Subscribers];
        for (int index = 0; index < Subscribers; index++)
        {
            MqttZeroSubscription subscription = new(
                "$share/workers/route/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                false,
                null);
            MqttZeroSessionState session = new(
                index.ToString(System.Globalization.CultureInfo.InvariantCulture));
            entries[index] = new MqttZeroSharedRouteEntry(session, [subscription]);
        }

        _snapshot = new MqttZeroSubscriptionRouteSnapshot(
            [],
            [new MqttZeroSharedRouteGroup("workers", entries)]);
    }

    /// <summary>
    /// Measures selecting one session from a shared group.
    /// </summary>
    /// <returns>The selected delivery count.</returns>
    [Benchmark]
    public int SelectSharedGroup()
    {
        using MqttZeroRouteScope route = MqttZeroRouteSelector.Select(
            _snapshot,
            Topic,
            null,
            _scopePool);
        return route.Count;
    }

    /// <summary>
    /// Releases pooled routing state.
    /// </summary>
    public void Dispose()
    {
        _scopePool.Dispose();
        GC.SuppressFinalize(this);
    }
}
