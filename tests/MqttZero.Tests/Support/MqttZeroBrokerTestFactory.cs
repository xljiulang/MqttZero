using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using MqttZero.Server.Storage;

namespace MqttZero.Tests.Support;

internal static class MqttZeroBrokerTestFactory
{
    internal static MqttZeroBroker CreateWithEnqueueInterceptor(
        IMqttZeroEnqueueInterceptor interceptor)
    {
        ArgumentNullException.ThrowIfNull(interceptor);
        return new MqttZeroBroker(
            Options.Create(new MqttZeroOptions()),
            TimeProvider.System,
            NullLogger<MqttZeroBroker>.Instance,
            [],
            [],
            [],
            [],
            [interceptor],
            [],
            [],
            new MqttZeroNoopSessionStore(),
            new MqttZeroNoopMessageStore(),
            new MqttZeroNoopRetainedMessageStore());
    }
}
