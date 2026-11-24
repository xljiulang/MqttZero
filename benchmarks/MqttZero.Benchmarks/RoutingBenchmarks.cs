using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using BenchmarkDotNet.Attributes;

namespace MqttZero.Benchmarks;

/// <summary>
/// Compares MQTTZero zero-allocation routing with MQTTnet topic matching.
/// </summary>
[MemoryDiagnoser]
public class RoutingBenchmarks : IDisposable
{
    private static readonly byte[] Topic = "route/topic"u8.ToArray();

    private readonly MqttZeroRouteScopePool _scopePool = new();

    private MqttZeroRouteEntry[] _entries = null!;
    private MqttZeroSubscriptionRouteSnapshot _snapshot = null!;
    private List<MqttZeroSessionState> _sessions = null!;

    /// <summary>
    /// Gets or sets the subscriber count.
    /// </summary>
    [Params(1, 100, 1000)]
    public int Subscribers { get; set; }

    /// <summary>
    /// Creates independent session subscriptions.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _sessions = new List<MqttZeroSessionState>(Subscribers);
        _entries = new MqttZeroRouteEntry[Subscribers];
        for (int index = 0; index < Subscribers; index++)
        {
            MqttZeroSessionState session = new(
                index.ToString(System.Globalization.CultureInfo.InvariantCulture));
            MqttZeroSubscription subscription = new(
                "route/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                false,
                null);
            session.Subscriptions.Add(subscription);
            _sessions.Add(session);
            _entries[index] = new MqttZeroRouteEntry(session, subscription);
        }

        _snapshot = new MqttZeroSubscriptionRouteSnapshot(
            [new MqttZeroRouteFilterGroup("route/#"u8.ToArray(), _entries)],
            []);
    }

    /// <summary>
    /// Measures MQTTZero subscription routing.
    /// </summary>
    /// <returns>The selected delivery count.</returns>
    [Benchmark(Baseline = true)]
    public int RouteMqttZero()
    {
        using MqttZeroRouteScope route = MqttZeroRouteSelector.Select(
            _snapshot,
            Topic,
            null,
            _scopePool);
        return route.Count;
    }

    /// <summary>
    /// Measures MQTTnet UTF-16 topic matching for the same subscriber count.
    /// </summary>
    /// <returns>The matched subscriber count.</returns>
    [Benchmark]
    public int MatchMqttNet()
    {
        int matches = 0;
        for (int index = 0; index < Subscribers; index++)
        {
            if (MQTTnet.MqttTopicFilterComparer.Compare(
                "route/topic",
                "route/#") == MQTTnet.MqttTopicFilterCompareResult.IsMatch)
            {
                matches++;
            }
        }

        return matches;
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
