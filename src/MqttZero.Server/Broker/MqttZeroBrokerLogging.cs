using Microsoft.Extensions.Logging;

namespace MqttZero.Server.Broker;

/// <summary>
/// Broker 的源生成日志方法。
/// </summary>
internal static partial class MqttZeroBrokerLogging
{
    /// <summary>
    /// 记录Maintenance失败。
    /// </summary>
    /// <param name="logger">日志记录器。</param>
    /// <param name="exception">异常。</param>
    [LoggerMessage(
        EventId = 1000,
        Level = LogLevel.Error,
        Message = "MQTTZero maintenance iteration failed.")]
    public static partial void LogMaintenanceFailure(
        ILogger<MqttZeroBroker> logger,
        Exception exception);

    /// <summary>
    /// 记录连接Cleanup失败。
    /// </summary>
    /// <param name="logger">日志记录器。</param>
    /// <param name="exception">异常。</param>
    [LoggerMessage(
        EventId = 1001,
        Level = LogLevel.Error,
        Message = "MQTTZero connection cleanup failed.")]
    public static partial void LogConnectionCleanupFailure(
        ILogger<MqttZeroBroker> logger,
        Exception exception);

    /// <summary>
    /// 记录观察者失败。
    /// </summary>
    /// <param name="logger">日志记录器。</param>
    /// <param name="exception">异常。</param>
    [LoggerMessage(
        EventId = 1002,
        Level = LogLevel.Error,
        Message = "An MQTTZero Observer threw an exception.")]
    public static partial void LogObserverFailure(
        ILogger<MqttZeroBroker> logger,
        Exception exception);

    /// <summary>
    /// 记录Persistence失败。
    /// </summary>
    /// <param name="logger">日志记录器。</param>
    /// <param name="operation">对应的 operation。</param>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="exception">异常。</param>
    [LoggerMessage(
        EventId = 1003,
        Level = LogLevel.Error,
        Message = "MQTTZero persistence operation {Operation} failed for client {ClientId}.")]
    public static partial void LogPersistenceFailure(
        ILogger<MqttZeroBroker> logger,
        string operation,
        string clientId,
        Exception exception);

    /// <summary>
    /// 记录出站失败。
    /// </summary>
    /// <param name="logger">日志记录器。</param>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="exception">异常。</param>
    [LoggerMessage(
        EventId = 1004,
        Level = LogLevel.Error,
        Message = "MQTTZero outbound pump failed for client {ClientId}.")]
    public static partial void LogEgressFailure(
        ILogger<MqttZeroBroker> logger,
        string clientId,
        Exception exception);

    /// <summary>
    /// 记录入站Dispatcher失败。
    /// </summary>
    /// <param name="logger">日志记录器。</param>
    /// <param name="exception">异常。</param>
    [LoggerMessage(
        EventId = 1005,
        Level = LogLevel.Error,
        Message = "MQTTZero inbound dispatcher failed.")]
    public static partial void LogInboundDispatcherFailure(
        ILogger<MqttZeroBroker> logger,
        Exception exception);

    /// <summary>
    /// 记录Fanout失败。
    /// </summary>
    /// <param name="logger">日志记录器。</param>
    /// <param name="exception">异常。</param>
    [LoggerMessage(
        EventId = 1006,
        Level = LogLevel.Error,
        Message = "MQTTZero sharded fanout failed.")]
    public static partial void LogFanoutFailure(
        ILogger<MqttZeroBroker> logger,
        Exception exception);

    /// <summary>
    /// 记录Invalid保留属性。
    /// </summary>
    /// <param name="logger">日志记录器。</param>
    /// <param name="topic">主题。</param>
    [LoggerMessage(
        EventId = 1007,
        Level = LogLevel.Warning,
        Message = "MQTTZero skipped a retained message with invalid properties on topic {Topic}.")]
    public static partial void LogInvalidRetainedProperties(
        ILogger<MqttZeroBroker> logger,
        string topic);
}
