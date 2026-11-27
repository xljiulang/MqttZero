using MqttZero.Server.Broker;
using System.Globalization;

namespace MqttZero.Server.Scheduling;

/// <summary>
/// 分片所有权的断言与违规诊断。
/// </summary>
internal static class MqttZeroShardOwnership
{
    /// <summary>
    /// 确保当前。
    /// </summary>
    /// <param name="shardId">分片Id。</param>
    /// <param name="operation">对应的 operation。</param>
    /// <param name="diagnostics">诊断。</param>
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
