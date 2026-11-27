using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// MQTT 5 CONNECT 报文。
/// </summary>
public sealed class MqttConnectPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.Connect;
        }
    }

    /// <summary>
    /// Gets or sets the negotiated protocol version.
    /// </summary>
    public MqttProtocolVersion ProtocolVersion { get; set; } =
        MqttProtocolVersion.V500;

    /// <summary>
    /// Gets or sets a value indicating whether the client requests a clean session.
    /// </summary>
    public bool CleanStart { get; set; }

    /// <summary>
    /// Gets or sets the keep-alive interval in seconds.
    /// </summary>
    public ushort KeepAliveSeconds { get; set; }

    /// <summary>
    /// Gets or sets the UTF-8 client identifier.
    /// </summary>
    public ReadOnlySequence<byte> ClientId { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the client requests response information.
    /// </summary>
    public bool RequestResponseInformation { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the server may send problem information.
    /// </summary>
    public bool RequestProblemInformation { get; set; } = true;

    /// <summary>
    /// Gets or sets the requested session expiry interval in seconds.
    /// </summary>
    public uint? SessionExpiryInterval { get; set; }

    /// <summary>
    /// Gets or sets the receive maximum requested by the client.
    /// </summary>
    public ushort ReceiveMaximum { get; set; } = ushort.MaxValue;

    /// <summary>
    /// Gets or sets the maximum packet size accepted by the client.
    /// </summary>
    public uint MaximumPacketSize { get; set; } = uint.MaxValue;

    /// <summary>
    /// Gets or sets the maximum inbound topic alias accepted by the client.
    /// </summary>
    public ushort TopicAliasMaximum { get; set; }

    /// <summary>
    /// Gets or sets the ordered CONNECT user properties.
    /// </summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; set; } =
        Array.Empty<MqttUserProperty>();

    /// <summary>
    /// Gets or sets the enhanced authentication method.
    /// </summary>
    public string? AuthenticationMethod { get; set; }

    /// <summary>
    /// Gets or sets the enhanced authentication data.
    /// </summary>
    public ReadOnlyMemory<byte>? AuthenticationData { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether a username is present.
    /// </summary>
    public bool HasUserName { get; set; }

    /// <summary>
    /// Gets or sets the UTF-8 username.
    /// </summary>
    public ReadOnlySequence<byte> UserName { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether a password is present.
    /// </summary>
    public bool HasPassword { get; set; }

    /// <summary>
    /// Gets or sets the password bytes.
    /// </summary>
    public ReadOnlySequence<byte> Password { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether a will message is present.
    /// </summary>
    public bool HasWill { get; set; }

    /// <summary>
    /// Gets or sets the will QoS.
    /// </summary>
    public MqttQualityOfServiceLevel WillQualityOfServiceLevel { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the will message is retained.
    /// </summary>
    public bool WillRetain { get; set; }

    /// <summary>
    /// Gets or sets the end-to-end Will message properties.
    /// </summary>
    public MqttPublishProperties WillProperties { get; set; } =
        MqttPublishProperties.Empty;

    /// <summary>
    /// Gets or sets the will delay interval in seconds.
    /// </summary>
    public uint WillDelayInterval { get; set; }

    /// <summary>
    /// Gets or sets the UTF-8 will topic.
    /// </summary>
    public ReadOnlySequence<byte> WillTopic { get; set; }

    /// <summary>
    /// Gets or sets the will payload.
    /// </summary>
    public ReadOnlySequence<byte> WillPayload { get; set; }

    internal uint WillMessageExpiryInterval
    {
        get
        {
            return WillProperties.MessageExpiryInterval ?? 0;
        }

        set
        {
            WillProperties = new MqttPublishProperties
            {
                PayloadFormatIndicator = WillProperties.PayloadFormatIndicator,
                MessageExpiryInterval = value == 0 ? null : value,
                ContentType = WillProperties.ContentType,
                ResponseTopic = WillProperties.ResponseTopic,
                CorrelationData = WillProperties.CorrelationData,
                UserProperties = WillProperties.UserProperties
            };
        }
    }
}
