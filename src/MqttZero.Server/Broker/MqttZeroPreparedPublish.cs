using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Interceptors;

namespace MqttZero.Server.Broker;

internal readonly struct MqttZeroPreparedPublish
{
    public MqttZeroPreparedPublish(
        MqttZeroMessageLease? lease,
        MqttZeroInterceptionResult result,
        MqttReasonCode reasonCode,
        bool usesSourceLease)
    {
        Lease = lease;
        Result = result;
        ReasonCode = reasonCode;
        UsesSourceLease = usesSourceLease;
    }

    public MqttZeroMessageLease? Lease { get; }

    public MqttZeroInterceptionResult Result { get; }

    public MqttReasonCode ReasonCode { get; }

    public bool UsesSourceLease { get; }
}
