namespace MqttZero.Server.Broker;

/// <summary>
/// 判断 QoS 0 发布能否绕过会话分片直接路由。
/// </summary>
internal static class MqttZeroQosZeroDirectRouting
{
    /// <summary>
    /// 获取是否可以直接路由该 QoS 0 发布而不经过分片。
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
