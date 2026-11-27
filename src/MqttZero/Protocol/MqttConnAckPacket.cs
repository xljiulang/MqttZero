using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// MQTT 5 CONNACK 报文。
/// </summary>
public sealed class MqttConnAckPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.ConnAck;
        }
    }

    /// <summary>
    /// Gets or sets a value indicating whether the server restored an existing session.
    /// </summary>
    public bool SessionPresent { get; set; }

    /// <summary>
    /// Gets or sets the connection reason code.
    /// </summary>
    public MqttReasonCode ReasonCode { get; set; }

    /// <summary>
    /// Gets or sets the maximum inbound topic alias accepted by the server.
    /// </summary>
    public ushort? TopicAliasMaximum { get; set; }

    /// <summary>
    /// Gets or sets the server receive maximum.
    /// </summary>
    public ushort? ReceiveMaximum { get; set; }

    /// <summary>
    /// Gets or sets the maximum packet size accepted by the server.
    /// </summary>
    public uint? MaximumPacketSize { get; set; }

    /// <summary>
    /// Gets or sets the optional reason string.
    /// </summary>
    public string? ReasonString { get; set; }

    /// <summary>
    /// Gets or sets the session expiry interval accepted by the server.
    /// </summary>
    public uint? SessionExpiryInterval { get; set; }

    /// <summary>
    /// Gets or sets the server-assigned client identifier.
    /// </summary>
    public string? AssignedClientIdentifier { get; set; }

    /// <summary>
    /// Gets or sets the maximum QoS supported by the server.
    /// </summary>
    public MqttQualityOfServiceLevel? MaximumQos { get; set; }

    /// <summary>
    /// Gets or sets whether retained messages are available.
    /// </summary>
    public bool? RetainAvailable { get; set; }

    /// <summary>
    /// Gets or sets whether wildcard subscriptions are available.
    /// </summary>
    public bool? WildcardSubscriptionAvailable { get; set; }

    /// <summary>
    /// Gets or sets whether subscription identifiers are available.
    /// </summary>
    public bool? SubscriptionIdentifierAvailable { get; set; }

    /// <summary>
    /// Gets or sets whether shared subscriptions are available.
    /// </summary>
    public bool? SharedSubscriptionAvailable { get; set; }

    /// <summary>
    /// Gets or sets the server keep-alive override in seconds.
    /// </summary>
    public ushort? ServerKeepAlive { get; set; }

    /// <summary>
    /// Gets or sets the response information.
    /// </summary>
    public string? ResponseInformation { get; set; }

    /// <summary>
    /// Gets or sets the server reference.
    /// </summary>
    public string? ServerReference { get; set; }

    /// <summary>
    /// Gets or sets the enhanced authentication method.
    /// </summary>
    public string? AuthenticationMethod { get; set; }

    /// <summary>
    /// Gets or sets the enhanced authentication data.
    /// </summary>
    public ReadOnlyMemory<byte>? AuthenticationData { get; set; }

    /// <summary>
    /// Gets or sets the ordered CONNACK user properties.
    /// </summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; set; } =
        Array.Empty<MqttUserProperty>();
}
