namespace MqttZero.Server.Broker;

/// <summary>
/// Decides whether one QoS 0 PUBLISH may skip the session shard and route from
/// the connection thread.
/// </summary>
internal static class MqttZeroQosZeroDirectRouting
{
    /// <summary>
    /// Gets a value indicating whether the direct QoS 0 route is safe.
    /// </summary>
    /// <param name="pendingPublishCount">
    /// The number of deferred publishes still owned by this connection.
    /// </param>
    /// <param name="retainFlag">Whether the message carries the retain flag.</param>
    /// <param name="anyStoreEnabled">Whether any persistence store is enabled.</param>
    /// <param name="anyExtensionRegistered">
    /// Whether any publish interceptor, enqueue interceptor or delivery observer
    /// is registered.
    /// </param>
    /// <returns><see langword="true"/> when the direct route preserves ordering.</returns>
    public static bool CanRoute(
        int pendingPublishCount,
        bool retainFlag,
        bool anyStoreEnabled,
        bool anyExtensionRegistered)
    {
        return pendingPublishCount == 0 &&
            !retainFlag &&
            !anyStoreEnabled &&
            !anyExtensionRegistered;
    }
}
