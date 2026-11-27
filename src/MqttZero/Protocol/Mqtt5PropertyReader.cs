using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using static MqttZero.Protocol.Mqtt5PropertyId;

namespace MqttZero.Protocol;

/// <summary>
/// MQTT 5 属性读取器。
/// </summary>
internal static class Mqtt5PropertyReader
{
    /// <summary>
    /// 尝试读取连接。
    /// </summary>
    /// <param name="properties">属性。</param>
    /// <param name="packet">报文。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public static bool TryReadConnect(
        ReadOnlySequence<byte> properties,
        MqttConnectPacket packet)
    {
        bool hasSessionExpiry = false;
        bool hasRequestProblemInformation = false;
        bool hasRequestResponseInformation = false;
        bool hasReceiveMaximum = false;
        bool hasMaximumPacketSize = false;
        bool hasTopicAliasMaximum = false;
        bool hasAuthenticationMethod = false;
        bool hasAuthenticationData = false;
        string? authenticationMethod = null;
        ReadOnlyMemory<byte>? authenticationData = null;
        List<MqttUserProperty>? userProperties = null;
        SequenceReader<byte> reader = new(properties);

        while (reader.Remaining > 0)
        {
            if (!reader.TryRead(out byte identifier))
            {
                return false;
            }

            switch (identifier)
            {
                case SessionExpiryInterval:
                    if (hasSessionExpiry || !TryReadUInt32(ref reader, out uint sessionExpiry))
                    {
                        return false;
                    }

                    hasSessionExpiry = true;
                    packet.SessionExpiryInterval = sessionExpiry;
                    break;
                case RequestProblemInformation:
                    if (hasRequestProblemInformation ||
                        !TryReadBoolean(ref reader, out bool requestProblemInformation))
                    {
                        return false;
                    }

                    hasRequestProblemInformation = true;
                    packet.RequestProblemInformation = requestProblemInformation;
                    break;
                case RequestResponseInformation:
                    if (hasRequestResponseInformation ||
                        !TryReadBoolean(ref reader, out bool requestResponseInformation))
                    {
                        return false;
                    }

                    hasRequestResponseInformation = true;
                    packet.RequestResponseInformation = requestResponseInformation;
                    break;
                case ReceiveMaximum:
                    if (hasReceiveMaximum ||
                        !MqttZeroWireFormat.TryReadUInt16(ref reader, out ushort receiveMaximum) ||
                        receiveMaximum == 0)
                    {
                        return false;
                    }

                    hasReceiveMaximum = true;
                    packet.ReceiveMaximum = receiveMaximum;
                    break;
                case MaximumPacketSize:
                    if (hasMaximumPacketSize ||
                        !TryReadUInt32(ref reader, out uint maximumPacketSize) ||
                        maximumPacketSize == 0)
                    {
                        return false;
                    }

                    hasMaximumPacketSize = true;
                    packet.MaximumPacketSize = maximumPacketSize;
                    break;
                case TopicAliasMaximum:
                    if (hasTopicAliasMaximum ||
                        !MqttZeroWireFormat.TryReadUInt16(ref reader, out ushort topicAliasMaximum))
                    {
                        return false;
                    }

                    hasTopicAliasMaximum = true;
                    packet.TopicAliasMaximum = topicAliasMaximum;
                    break;
                case AuthenticationMethod:
                    if (hasAuthenticationMethod ||
                        !TryReadUtf8String(ref reader, out string? readAuthenticationMethod) ||
                        string.IsNullOrEmpty(readAuthenticationMethod))
                    {
                        return false;
                    }

                    hasAuthenticationMethod = true;
                    authenticationMethod = readAuthenticationMethod;
                    break;
                case AuthenticationData:
                    if (hasAuthenticationData ||
                        !TryReadBinary(ref reader, out ReadOnlyMemory<byte> readAuthenticationData))
                    {
                        return false;
                    }

                    hasAuthenticationData = true;
                    authenticationData = readAuthenticationData;
                    break;
                case UserProperty:
                    if (!TryReadUserProperty(ref reader, out MqttUserProperty? userProperty))
                    {
                        return false;
                    }

                    userProperties ??= [];
                    userProperties.Add(userProperty);
                    break;
                default:
                    if (!Mqtt5PropertySchema.IsAllowed(MqttPacketType.Connect, identifier) ||
                        !Mqtt5PropertyLayout.TrySkip(identifier, ref reader))
                    {
                        return false;
                    }

                    break;
            }
        }

        if (authenticationData.HasValue && !hasAuthenticationMethod)
        {
            return false;
        }

        packet.AuthenticationMethod = authenticationMethod;
        packet.AuthenticationData = authenticationData;
        packet.UserProperties = (IReadOnlyList<MqttUserProperty>?)userProperties ??
            Array.Empty<MqttUserProperty>();
        return true;
    }

    /// <summary>
    /// 尝试读取遗嘱。
    /// </summary>
    /// <param name="properties">属性。</param>
    /// <param name="packet">报文。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public static bool TryReadWill(
        ReadOnlySequence<byte> properties,
        MqttConnectPacket packet)
    {
        if (!TryReadMessageProperties(
                properties,
                isWill: true,
                out MqttPublishProperties publishProperties,
                out _,
                out _,
                out uint willDelay))
        {
            return false;
        }

        packet.WillProperties = publishProperties;
        packet.WillDelayInterval = willDelay;
        return true;
    }

    /// <summary>
    /// 尝试读取发布。
    /// </summary>
    /// <param name="properties">属性。</param>
    /// <param name="packet">报文。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public static bool TryReadPublish(
        ReadOnlySequence<byte> properties,
        MqttPublishPacket packet)
    {
        if (!TryReadMessageProperties(
                properties,
                isWill: false,
                out MqttPublishProperties publishProperties,
                out IReadOnlyList<uint> subscriptionIdentifiers,
                out ushort? topicAlias,
                out _))
        {
            return false;
        }

        packet.Properties = publishProperties;
        packet.SubscriptionIdentifiers = subscriptionIdentifiers;
        packet.TopicAlias = topicAlias;
        return true;
    }

    /// <summary>
    /// 尝试读取Conn确认。
    /// </summary>
    /// <param name="properties">属性。</param>
    /// <param name="packet">报文。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public static bool TryReadConnAck(
        ReadOnlySequence<byte> properties,
        MqttConnAckPacket packet)
    {
        bool hasTopicAliasMaximum = false;
        bool hasReceiveMaximum = false;
        bool hasMaximumPacketSize = false;
        bool hasSessionExpiry = false;
        bool hasAssignedClientIdentifier = false;
        bool hasMaximumQos = false;
        bool hasRetainAvailable = false;
        bool hasWildcardSubscriptionAvailable = false;
        bool hasSubscriptionIdentifierAvailable = false;
        bool hasSharedSubscriptionAvailable = false;
        bool hasServerKeepAlive = false;
        bool hasResponseInformation = false;
        bool hasServerReference = false;
        bool hasAuthenticationMethod = false;
        bool hasAuthenticationData = false;
        bool hasReasonString = false;
        string? authenticationMethod = null;
        ReadOnlyMemory<byte>? authenticationData = null;
        List<MqttUserProperty>? userProperties = null;
        SequenceReader<byte> reader = new(properties);
        while (reader.Remaining > 0)
        {
            if (!reader.TryRead(out byte identifier))
            {
                return false;
            }

            switch (identifier)
            {
                case TopicAliasMaximum:
                    if (hasTopicAliasMaximum ||
                        !MqttZeroWireFormat.TryReadUInt16(ref reader, out ushort topicAliasMaximum))
                    {
                        return false;
                    }

                    hasTopicAliasMaximum = true;
                    packet.TopicAliasMaximum = topicAliasMaximum;
                    break;
                case ReceiveMaximum:
                    if (hasReceiveMaximum ||
                        !MqttZeroWireFormat.TryReadUInt16(ref reader, out ushort receiveMaximum) ||
                        receiveMaximum == 0)
                    {
                        return false;
                    }

                    hasReceiveMaximum = true;
                    packet.ReceiveMaximum = receiveMaximum;
                    break;
                case MaximumPacketSize:
                    if (hasMaximumPacketSize ||
                        !TryReadUInt32(ref reader, out uint maximumPacketSize) ||
                        maximumPacketSize == 0)
                    {
                        return false;
                    }

                    hasMaximumPacketSize = true;
                    packet.MaximumPacketSize = maximumPacketSize;
                    break;
                case SessionExpiryInterval:
                    if (hasSessionExpiry ||
                        !TryReadUInt32(ref reader, out uint sessionExpiry))
                    {
                        return false;
                    }

                    hasSessionExpiry = true;
                    packet.SessionExpiryInterval = sessionExpiry;
                    break;
                case AssignedClientIdentifier:
                    if (hasAssignedClientIdentifier ||
                        !TryReadUtf8String(ref reader, out string? assignedClientIdentifier) ||
                        string.IsNullOrEmpty(assignedClientIdentifier))
                    {
                        return false;
                    }

                    hasAssignedClientIdentifier = true;
                    packet.AssignedClientIdentifier = assignedClientIdentifier;
                    break;
                case MaximumQos:
                    if (hasMaximumQos || !reader.TryRead(out byte maximumQos) || maximumQos > 1)
                    {
                        return false;
                    }

                    hasMaximumQos = true;
                    packet.MaximumQos = (MqttQualityOfServiceLevel)maximumQos;
                    break;
                case RetainAvailable:
                    if (hasRetainAvailable ||
                        !TryReadBoolean(ref reader, out bool retainAvailable))
                    {
                        return false;
                    }

                    hasRetainAvailable = true;
                    packet.RetainAvailable = retainAvailable;
                    break;
                case WildcardSubscriptionAvailable:
                    if (hasWildcardSubscriptionAvailable ||
                        !TryReadBoolean(ref reader, out bool wildcardSubscriptionAvailable))
                    {
                        return false;
                    }

                    hasWildcardSubscriptionAvailable = true;
                    packet.WildcardSubscriptionAvailable = wildcardSubscriptionAvailable;
                    break;
                case SubscriptionIdentifierAvailable:
                    if (hasSubscriptionIdentifierAvailable ||
                        !TryReadBoolean(
                            ref reader,
                            out bool subscriptionIdentifierAvailable))
                    {
                        return false;
                    }

                    hasSubscriptionIdentifierAvailable = true;
                    packet.SubscriptionIdentifierAvailable = subscriptionIdentifierAvailable;
                    break;
                case SharedSubscriptionAvailable:
                    if (hasSharedSubscriptionAvailable ||
                        !TryReadBoolean(ref reader, out bool sharedSubscriptionAvailable))
                    {
                        return false;
                    }

                    hasSharedSubscriptionAvailable = true;
                    packet.SharedSubscriptionAvailable = sharedSubscriptionAvailable;
                    break;
                case ServerKeepAlive:
                    if (hasServerKeepAlive ||
                        !MqttZeroWireFormat.TryReadUInt16(ref reader, out ushort serverKeepAlive))
                    {
                        return false;
                    }

                    hasServerKeepAlive = true;
                    packet.ServerKeepAlive = serverKeepAlive;
                    break;
                case ResponseInformation:
                    if (hasResponseInformation ||
                        !TryReadUtf8String(ref reader, out string? responseInformation))
                    {
                        return false;
                    }

                    hasResponseInformation = true;
                    packet.ResponseInformation = responseInformation;
                    break;
                case ServerReference:
                    if (hasServerReference ||
                        !TryReadUtf8String(ref reader, out string? serverReference))
                    {
                        return false;
                    }

                    hasServerReference = true;
                    packet.ServerReference = serverReference;
                    break;
                case AuthenticationMethod:
                    if (hasAuthenticationMethod ||
                        !TryReadUtf8String(ref reader, out string? readAuthenticationMethod) ||
                        string.IsNullOrEmpty(readAuthenticationMethod))
                    {
                        return false;
                    }

                    hasAuthenticationMethod = true;
                    authenticationMethod = readAuthenticationMethod;
                    break;
                case AuthenticationData:
                    if (hasAuthenticationData ||
                        !TryReadBinary(ref reader, out ReadOnlyMemory<byte> readAuthenticationData))
                    {
                        return false;
                    }

                    hasAuthenticationData = true;
                    authenticationData = readAuthenticationData;
                    break;
                case ReasonString:
                    if (hasReasonString ||
                        !TryReadUtf8String(ref reader, out string? reasonString))
                    {
                        return false;
                    }

                    hasReasonString = true;
                    packet.ReasonString = reasonString;
                    break;
                case UserProperty:
                    if (!TryReadUserProperty(ref reader, out MqttUserProperty? userProperty))
                    {
                        return false;
                    }

                    userProperties ??= [];
                    userProperties.Add(userProperty);
                    break;
                default:
                    if (!Mqtt5PropertySchema.IsAllowed(MqttPacketType.ConnAck, identifier) ||
                        !Mqtt5PropertyLayout.TrySkip(identifier, ref reader))
                    {
                        return false;
                    }

                    break;
            }
        }

        if (authenticationData.HasValue && !hasAuthenticationMethod)
        {
            return false;
        }

        packet.AuthenticationMethod = authenticationMethod;
        packet.AuthenticationData = authenticationData;
        packet.UserProperties = (IReadOnlyList<MqttUserProperty>?)userProperties ??
            Array.Empty<MqttUserProperty>();
        return true;
    }

    /// <summary>
    /// 尝试读取订阅。
    /// </summary>
    /// <param name="properties">属性。</param>
    /// <param name="packet">报文。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public static bool TryReadSubscribe(
        ReadOnlySequence<byte> properties,
        MqttSubscribePacket packet)
    {
        List<MqttUserProperty>? userProperties = null;
        SequenceReader<byte> reader = new(properties);
        while (reader.Remaining > 0)
        {
            if (!reader.TryRead(out byte identifier))
            {
                return false;
            }

            if (identifier == SubscriptionIdentifier)
            {
                if (packet.SubscriptionIdentifier.HasValue ||
                    !MqttZeroWireFormat.TryReadVariableByteInteger(ref reader, out int subscriptionIdentifier) ||
                    subscriptionIdentifier == 0)
                {
                    return false;
                }

                packet.SubscriptionIdentifier = (uint)subscriptionIdentifier;
            }
            else if (identifier == UserProperty)
            {
                if (!TryReadUserProperty(ref reader, out MqttUserProperty? userProperty))
                {
                    return false;
                }

                userProperties ??= [];
                userProperties.Add(userProperty);
            }
            else if (!Mqtt5PropertySchema.IsAllowed(MqttPacketType.Subscribe, identifier) ||
                !Mqtt5PropertyLayout.TrySkip(identifier, ref reader))
            {
                return false;
            }
        }

        packet.UserProperties = (IReadOnlyList<MqttUserProperty>?)userProperties ??
            Array.Empty<MqttUserProperty>();
        return true;
    }

    /// <summary>
    /// 尝试读取退订。
    /// </summary>
    /// <param name="properties">属性。</param>
    /// <param name="packet">报文。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public static bool TryReadUnsubscribe(
        ReadOnlySequence<byte> properties,
        MqttUnsubscribePacket packet)
    {
        List<MqttUserProperty>? userProperties = null;
        SequenceReader<byte> reader = new(properties);
        while (reader.Remaining > 0)
        {
            if (!reader.TryRead(out byte identifier))
            {
                return false;
            }

            if (identifier == UserProperty)
            {
                if (!TryReadUserProperty(ref reader, out MqttUserProperty? userProperty))
                {
                    return false;
                }

                userProperties ??= [];
                userProperties.Add(userProperty);
            }
            else
            {
                return false;
            }
        }

        packet.UserProperties = (IReadOnlyList<MqttUserProperty>?)userProperties ??
            Array.Empty<MqttUserProperty>();
        return true;
    }

    /// <summary>
    /// 尝试读取原因属性。
    /// </summary>
    /// <param name="properties">属性。</param>
    /// <param name="reasonString">原因字符串。</param>
    /// <param name="userProperties">用户属性。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public static bool TryReadReasonProperties(
        ReadOnlySequence<byte> properties,
        out string? reasonString,
        out IReadOnlyList<MqttUserProperty> userProperties)
    {
        reasonString = null;
        List<MqttUserProperty>? values = null;
        SequenceReader<byte> reader = new(properties);
        while (reader.Remaining > 0)
        {
            if (!reader.TryRead(out byte identifier))
            {
                userProperties = Array.Empty<MqttUserProperty>();
                return false;
            }

            if (identifier == ReasonString)
            {
                if (reasonString != null ||
                    !TryReadUtf8String(ref reader, out string? readReasonString))
                {
                    userProperties = Array.Empty<MqttUserProperty>();
                    return false;
                }

                reasonString = readReasonString;
            }
            else if (identifier == UserProperty)
            {
                if (!TryReadUserProperty(ref reader, out MqttUserProperty? userProperty))
                {
                    userProperties = Array.Empty<MqttUserProperty>();
                    return false;
                }

                values ??= [];
                values.Add(userProperty);
            }
            else
            {
                userProperties = Array.Empty<MqttUserProperty>();
                return false;
            }
        }

        userProperties = (IReadOnlyList<MqttUserProperty>?)values ??
            Array.Empty<MqttUserProperty>();
        return true;
    }

    /// <summary>
    /// 尝试Validate原因属性。
    /// </summary>
    /// <param name="properties">属性。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    internal static bool TryValidateReasonProperties(ReadOnlySequence<byte> properties)
    {
        bool hasReasonString = false;
        SequenceReader<byte> reader = new(properties);
        while (reader.Remaining > 0)
        {
            if (!reader.TryRead(out byte identifier))
            {
                return false;
            }

            if (identifier == ReasonString)
            {
                if (hasReasonString ||
                    !TrySkipValidatedUtf8(ref reader))
                {
                    return false;
                }

                hasReasonString = true;
            }
            else if (identifier == UserProperty)
            {
                if (!TrySkipValidatedUtf8(ref reader) ||
                    !TrySkipValidatedUtf8(ref reader))
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// 尝试读取断开。
    /// </summary>
    /// <param name="properties">属性。</param>
    /// <param name="packet">报文。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public static bool TryReadDisconnect(
        ReadOnlySequence<byte> properties,
        MqttDisconnectPacket packet)
    {
        bool hasSessionExpiry = false;
        bool hasReasonString = false;
        bool hasServerReference = false;
        List<MqttUserProperty>? userProperties = null;
        SequenceReader<byte> reader = new(properties);
        while (reader.Remaining > 0)
        {
            if (!reader.TryRead(out byte identifier))
            {
                return false;
            }

            if (identifier == SessionExpiryInterval)
            {
                if (hasSessionExpiry ||
                    !TryReadUInt32(ref reader, out uint sessionExpiry))
                {
                    return false;
                }

                hasSessionExpiry = true;
                packet.SessionExpiryInterval = sessionExpiry;
            }
            else if (identifier == ReasonString)
            {
                if (hasReasonString ||
                    !TryReadUtf8String(ref reader, out string? reasonString))
                {
                    return false;
                }

                hasReasonString = true;
                packet.ReasonString = reasonString;
            }
            else if (identifier == ServerReference)
            {
                if (hasServerReference ||
                    !TryReadUtf8String(ref reader, out string? serverReference))
                {
                    return false;
                }

                hasServerReference = true;
                packet.ServerReference = serverReference;
            }
            else if (identifier == UserProperty)
            {
                if (!TryReadUserProperty(ref reader, out MqttUserProperty? userProperty))
                {
                    return false;
                }

                userProperties ??= [];
                userProperties.Add(userProperty);
            }
            else if (!Mqtt5PropertySchema.IsAllowed(MqttPacketType.Disconnect, identifier) ||
                !Mqtt5PropertyLayout.TrySkip(identifier, ref reader))
            {
                return false;
            }
        }

        packet.UserProperties = (IReadOnlyList<MqttUserProperty>?)userProperties ??
            Array.Empty<MqttUserProperty>();
        return true;
    }

    /// <summary>
    /// 尝试读取消息属性。
    /// </summary>
    /// <param name="properties">属性。</param>
    /// <param name="isWill">is遗嘱。</param>
    /// <param name="publishProperties">发布属性。</param>
    /// <param name="subscriptionIdentifiers">订阅标识符。</param>
    /// <param name="topicAlias">主题别名。</param>
    /// <param name="willDelay">遗嘱Delay。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool TryReadMessageProperties(
        ReadOnlySequence<byte> properties,
        bool isWill,
        out MqttPublishProperties publishProperties,
        out IReadOnlyList<uint> subscriptionIdentifiers,
        out ushort? topicAlias,
        out uint willDelay)
    {
        MqttPayloadFormatIndicator? payloadFormat = null;
        bool hasMessageExpiry = false;
        uint messageExpiry = 0;
        string? contentType = null;
        string? responseTopic = null;
        ReadOnlyMemory<byte>? correlationData = null;
        List<uint>? subscriptionIdentifierValues = null;
        topicAlias = null;
        willDelay = 0;
        bool hasWillDelay = false;
        List<MqttUserProperty>? userProperties = null;
        SequenceReader<byte> reader = new(properties);

        while (reader.Remaining > 0)
        {
            if (!reader.TryRead(out byte identifier))
            {
                publishProperties = MqttPublishProperties.Empty;
                subscriptionIdentifiers = Array.Empty<uint>();
                return false;
            }

            switch (identifier)
            {
                case PayloadFormatIndicator:
                    if (payloadFormat.HasValue || !reader.TryRead(out byte encodedFormat) ||
                        encodedFormat > (byte)MqttPayloadFormatIndicator.Utf8)
                    {
                        publishProperties = MqttPublishProperties.Empty;
                        subscriptionIdentifiers = Array.Empty<uint>();
                        return false;
                    }

                    payloadFormat = (MqttPayloadFormatIndicator)encodedFormat;
                    break;
                case MessageExpiryInterval:
                    if (hasMessageExpiry ||
                        !TryReadUInt32(ref reader, out messageExpiry))
                    {
                        publishProperties = MqttPublishProperties.Empty;
                        subscriptionIdentifiers = Array.Empty<uint>();
                        return false;
                    }

                    hasMessageExpiry = true;
                    break;
                case ContentType:
                    if (contentType != null ||
                        !TryReadUtf8String(ref reader, out string? readContentType))
                    {
                        publishProperties = MqttPublishProperties.Empty;
                        subscriptionIdentifiers = Array.Empty<uint>();
                        return false;
                    }

                    contentType = readContentType;
                    break;
                case ResponseTopic:
                    if (responseTopic != null ||
                        !TryReadUtf8String(
                            ref reader,
                            out string? readResponseTopic,
                            isTopicName: true))
                    {
                        publishProperties = MqttPublishProperties.Empty;
                        subscriptionIdentifiers = Array.Empty<uint>();
                        return false;
                    }

                    responseTopic = readResponseTopic;
                    break;
                case CorrelationData:
                    if (correlationData.HasValue ||
                        !TryReadBinary(ref reader, out ReadOnlyMemory<byte> readCorrelationData))
                    {
                        publishProperties = MqttPublishProperties.Empty;
                        subscriptionIdentifiers = Array.Empty<uint>();
                        return false;
                    }

                    correlationData = readCorrelationData;
                    break;
                case SubscriptionIdentifier when !isWill:
                    if (!MqttZeroWireFormat.TryReadVariableByteInteger(
                            ref reader,
                            out int subscriptionIdentifier) ||
                        subscriptionIdentifier == 0)
                    {
                        publishProperties = MqttPublishProperties.Empty;
                        subscriptionIdentifiers = Array.Empty<uint>();
                        return false;
                    }

                    subscriptionIdentifierValues ??= [];
                    subscriptionIdentifierValues.Add((uint)subscriptionIdentifier);
                    break;
                case TopicAlias when !isWill:
                    if (topicAlias.HasValue ||
                        !MqttZeroWireFormat.TryReadUInt16(ref reader, out ushort readTopicAlias) ||
                        readTopicAlias == 0)
                    {
                        publishProperties = MqttPublishProperties.Empty;
                        subscriptionIdentifiers = Array.Empty<uint>();
                        return false;
                    }

                    topicAlias = readTopicAlias;
                    break;
                case WillDelayInterval when isWill:
                    if (hasWillDelay || !TryReadUInt32(ref reader, out willDelay))
                    {
                        publishProperties = MqttPublishProperties.Empty;
                        subscriptionIdentifiers = Array.Empty<uint>();
                        return false;
                    }

                    hasWillDelay = true;
                    break;
                case UserProperty:
                    if (!TryReadUserProperty(
                            ref reader,
                            out MqttUserProperty? userProperty))
                    {
                        publishProperties = MqttPublishProperties.Empty;
                        subscriptionIdentifiers = Array.Empty<uint>();
                        return false;
                    }

                    userProperties ??= [];
                    userProperties.Add(userProperty);
                    break;
                default:
                    bool allowed = isWill
                        ? Mqtt5PropertySchema.IsAllowedWillProperty(identifier)
                        : Mqtt5PropertySchema.IsAllowed(MqttPacketType.Publish, identifier);
                    if (!allowed || !Mqtt5PropertyLayout.TrySkip(identifier, ref reader))
                    {
                        publishProperties = MqttPublishProperties.Empty;
                        subscriptionIdentifiers = Array.Empty<uint>();
                        return false;
                    }

                    break;
            }
        }

        publishProperties = new MqttPublishProperties
        {
            PayloadFormatIndicator = payloadFormat,
            MessageExpiryInterval = hasMessageExpiry ? messageExpiry : null,
            ContentType = contentType,
            ResponseTopic = responseTopic,
            CorrelationData = correlationData,
            UserProperties = (IReadOnlyList<MqttUserProperty>?)userProperties ??
                Array.Empty<MqttUserProperty>()
        };
        subscriptionIdentifiers =
            (IReadOnlyList<uint>?)subscriptionIdentifierValues ?? Array.Empty<uint>();
        return true;
    }

    /// <summary>
    /// 尝试读取用户属性。
    /// </summary>
    /// <param name="reader">读取器。</param>
    /// <param name="userProperty">用户属性。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool TryReadUserProperty(
        ref SequenceReader<byte> reader,
        [NotNullWhen(true)] out MqttUserProperty? userProperty)
    {
        userProperty = null;
        if (!TryReadUtf8String(ref reader, out string? name) ||
            !TryReadUtf8String(ref reader, out string? value) ||
            name == null ||
            value == null)
        {
            return false;
        }

        userProperty = new MqttUserProperty(name, value);
        return true;
    }

    /// <summary>
    /// 尝试读取布尔值。
    /// </summary>
    /// <param name="reader">读取器。</param>
    /// <param name="value">值。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool TryReadBoolean(ref SequenceReader<byte> reader, out bool value)
    {
        value = false;
        if (!reader.TryRead(out byte encoded) || encoded > 1)
        {
            return false;
        }

        value = encoded == 1;
        return true;
    }

    /// <summary>
    /// 尝试读取二进制。
    /// </summary>
    /// <param name="reader">读取器。</param>
    /// <param name="value">值。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool TryReadBinary(
        ref SequenceReader<byte> reader,
        out ReadOnlyMemory<byte> value)
    {
        value = ReadOnlyMemory<byte>.Empty;
        if (!MqttZeroWireFormat.TryReadUInt16(ref reader, out ushort length) ||
            reader.Remaining < length)
        {
            return false;
        }

        value = length == 0
            ? ReadOnlyMemory<byte>.Empty
            : reader.Sequence.Slice(reader.Position, length).ToArray();
        reader.Advance(length);
        return true;
    }

    /// <summary>
    /// 尝试SkipValidatedUTF-8。
    /// </summary>
    /// <param name="reader">读取器。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool TrySkipValidatedUtf8(ref SequenceReader<byte> reader)
    {
        if (!MqttZeroWireFormat.TryReadUInt16(ref reader, out ushort length) ||
            reader.Remaining < length)
        {
            return false;
        }

        ReadOnlySequence<byte> encoded = length == 0
            ? ReadOnlySequence<byte>.Empty
            : reader.Sequence.Slice(reader.Position, length);
        if (!MqttZeroProtocolValidator.IsValidUtf8(encoded))
        {
            return false;
        }

        reader.Advance(length);
        return true;
    }

    /// <summary>
    /// 尝试读取UTF-8字符串。
    /// </summary>
    /// <param name="reader">读取器。</param>
    /// <param name="value">值。</param>
    /// <param name="isTopicName">is主题名称。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool TryReadUtf8String(
        ref SequenceReader<byte> reader,
        out string? value,
        bool isTopicName = false)
    {
        value = null;
        if (!MqttZeroWireFormat.TryReadUInt16(ref reader, out ushort length) ||
            reader.Remaining < length)
        {
            return false;
        }

        ReadOnlySequence<byte> encoded = length == 0
            ? ReadOnlySequence<byte>.Empty
            : reader.Sequence.Slice(reader.Position, length);
        if (!MqttZeroProtocolValidator.IsValidUtf8(encoded))
        {
            return false;
        }

        if (isTopicName && !MqttZeroProtocolValidator.IsValidTopicName(encoded))
        {
            return false;
        }

        value = length == 0
            ? string.Empty
            : Encoding.UTF8.GetString(encoded);
        reader.Advance(length);
        return true;
    }

    /// <summary>
    /// 尝试读取UInt32。
    /// </summary>
    /// <param name="reader">读取器。</param>
    /// <param name="value">值。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool TryReadUInt32(ref SequenceReader<byte> reader, out uint value)
    {
        value = 0;
        if (!reader.TryRead(out byte b1) ||
            !reader.TryRead(out byte b2) ||
            !reader.TryRead(out byte b3) ||
            !reader.TryRead(out byte b4))
        {
            return false;
        }

        value = BinaryPrimitives.ReadUInt32BigEndian([b1, b2, b3, b4]);
        return true;
    }

}
