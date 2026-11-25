namespace MqttZero.Server.Broker;

internal static class MqttZeroOptionsValidator
{
    private static readonly System.Text.UTF8Encoding StrictUtf8 =
        new(false, true);

    public static void Validate(MqttZeroOptions options)
    {
        ValidatePositive(
            options.MaxActiveConnections,
            nameof(options.MaxActiveConnections));
        ValidatePositive(options.MaxSessions, nameof(options.MaxSessions));
        if (options.EventLoopShardCount < 0 ||
            options.EventLoopShardCount > 256)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "MqttZeroOptions.EventLoopShardCount must be between 0 and 256.");
        }

        ValidatePositive(
            options.MaxSubscriptionsPerSession,
            nameof(options.MaxSubscriptionsPerSession));
        ValidatePositive(options.MaxInflight, nameof(options.MaxInflight));
        ValidatePositive(
            options.OutboundBatchBytes,
            nameof(options.OutboundBatchBytes));
        ValidatePositive(
            options.OutboundBatchMessages,
            nameof(options.OutboundBatchMessages));
        ValidatePositive(
            options.OutboundBatchDuration,
            nameof(options.OutboundBatchDuration));
        if (options.OutboundBatchBytes > options.MaxBufferedBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "MqttZeroOptions.OutboundBatchBytes cannot exceed MaxBufferedBytes.");
        }

        ValidatePositive(options.MaxAwaitingRel, nameof(options.MaxAwaitingRel));
        ValidatePositive(
            options.MaxMessageQueueLength,
            nameof(options.MaxMessageQueueLength));
        ValidatePositive(options.MaxPacketSize, nameof(options.MaxPacketSize));
        if (options.MaxBufferedBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "MqttZeroOptions.MaxBufferedBytes must be positive.");
        }

        if (options.MaxRetainedMessages < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "MqttZeroOptions.MaxRetainedMessages cannot be negative.");
        }
        if (options.MaxRetainedBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "MqttZeroOptions.MaxRetainedBytes must be positive.");
        }

        ValidatePositive(options.ConnectTimeout, nameof(options.ConnectTimeout));
        ValidatePositive(
            options.IncompletePacketTimeout,
            nameof(options.IncompletePacketTimeout));
        ValidatePositive(
            options.KeepAliveCheckInterval,
            nameof(options.KeepAliveCheckInterval));
        if (options.MaxSessionExpiryInterval.HasValue &&
            options.MaxSessionExpiryInterval.Value < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "MqttZeroOptions.MaxSessionExpiryInterval cannot be negative.");
        }

        ValidateUtf8String(
            options.ResponseInformation,
            nameof(options.ResponseInformation));
    }

    private static void ValidatePositive(int value, string propertyName)
    {
        if (value <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(value),
                $"MqttZeroOptions.{propertyName} must be positive.");
        }
    }

    private static void ValidatePositive(TimeSpan value, string propertyName)
    {
        if (value <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(value),
                $"MqttZeroOptions.{propertyName} must be positive.");
        }
    }

    private static void ValidateUtf8String(string? value, string propertyName)
    {
        if (value == null)
        {
            return;
        }

        if (value.Contains('\0', StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"MqttZeroOptions.{propertyName} cannot contain NUL.",
                nameof(value));
        }

        try
        {
            if (StrictUtf8.GetByteCount(value) > ushort.MaxValue)
            {
                throw new ArgumentException(
                    $"MqttZeroOptions.{propertyName} exceeds the MQTT UTF-8 length limit.",
                    nameof(value));
            }
        }
        catch (System.Text.EncoderFallbackException exception)
        {
            throw new ArgumentException(
                $"MqttZeroOptions.{propertyName} contains invalid Unicode.",
                nameof(value),
                exception);
        }
    }
}
