using Microsoft.Extensions.Logging;

namespace MqttZero.Server.Broker;

internal static partial class MqttZeroBrokerLogging
{
    [LoggerMessage(
        EventId = 1000,
        Level = LogLevel.Error,
        Message = "MQTTZero maintenance iteration failed.")]
    public static partial void LogMaintenanceFailure(
        ILogger<MqttZeroBroker> logger,
        Exception exception);

    [LoggerMessage(
        EventId = 1001,
        Level = LogLevel.Error,
        Message = "MQTTZero connection cleanup failed.")]
    public static partial void LogConnectionCleanupFailure(
        ILogger<MqttZeroBroker> logger,
        Exception exception);

    [LoggerMessage(
        EventId = 1002,
        Level = LogLevel.Error,
        Message = "An MQTTZero Observer threw an exception.")]
    public static partial void LogObserverFailure(
        ILogger<MqttZeroBroker> logger,
        Exception exception);

    [LoggerMessage(
        EventId = 1003,
        Level = LogLevel.Error,
        Message = "MQTTZero persistence operation {Operation} failed for client {ClientId}.")]
    public static partial void LogPersistenceFailure(
        ILogger<MqttZeroBroker> logger,
        string operation,
        string clientId,
        Exception exception);

    [LoggerMessage(
        EventId = 1004,
        Level = LogLevel.Error,
        Message = "MQTTZero outbound pump failed for client {ClientId}.")]
    public static partial void LogEgressFailure(
        ILogger<MqttZeroBroker> logger,
        string clientId,
        Exception exception);

    [LoggerMessage(
        EventId = 1005,
        Level = LogLevel.Error,
        Message = "MQTTZero inbound dispatcher failed.")]
    public static partial void LogInboundDispatcherFailure(
        ILogger<MqttZeroBroker> logger,
        Exception exception);

    [LoggerMessage(
        EventId = 1006,
        Level = LogLevel.Error,
        Message = "MQTTZero sharded fanout failed.")]
    public static partial void LogFanoutFailure(
        ILogger<MqttZeroBroker> logger,
        Exception exception);
}
