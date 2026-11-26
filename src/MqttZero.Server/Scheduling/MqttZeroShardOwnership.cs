using MqttZero.Server.Broker;
using System.Globalization;

namespace MqttZero.Server.Scheduling;

internal static class MqttZeroShardOwnership
{
    internal static void EnsureCurrent(
        int shardId,
        string operation,
        MqttZeroFanoutDiagnostics? diagnostics = null)
    {
        if (MqttZeroEventLoop.CurrentShardId == shardId)
        {
            return;
        }

        diagnostics?.RecordShardOwnershipViolation();
        int? currentShardId = MqttZeroEventLoop.CurrentShardId;
        throw new InvalidOperationException(
            $"{operation} must execute on shard {shardId}, " +
            $"but the current shard is " +
            $"{(currentShardId.HasValue
                ? currentShardId.Value.ToString(CultureInfo.InvariantCulture)
                : "none")}.");
    }
}
