using MqttQualityOfServiceLevel = MqttZero.Protocol.MqttQualityOfServiceLevel;
using MqttZero.Server;
using MqttZero.AspNetCore;
using MqttZero.Server.Broker;
using MqttZero.Server.Retained;
using MqttZero.Server.Storage;
using MqttZero.Publishing;
using Microsoft.Extensions.DependencyInjection;
using System.Buffers;

namespace MqttZero.Tests.Broker
{
    public sealed class BrokerInterfaceTests
    {
        [Fact]
        public async Task AddMqttZeroRegistersConcreteAndInterfaceAsSameSingleton()
        {
            ServiceCollection services = new();
            services.AddMqttZero(options => options.MaxPacketSize = 256);
            await using ServiceProvider provider = services.BuildServiceProvider();

            MqttZeroBroker concrete = provider.GetRequiredService<MqttZeroBroker>();
            IMqttZeroBroker broker = provider.GetRequiredService<IMqttZeroBroker>();

            Assert.Same(concrete, broker);
            Assert.Equal(256, broker.MaximumPacketSize);
        }

        [Fact]
        public async Task InterfaceConveniencePublishUsesTheBrokerPipeline()
        {
            MqttZeroBroker concrete = MqttZeroBroker.Create(
                new MqttZeroOptions(),
                TimeProvider.System);
            await concrete.StartAsync(TestContext.Current.CancellationToken);
            try
            {
                await ((IMqttZeroBroker)concrete).PublishAsync(
                    "api/interface",
                    new ReadOnlySequence<byte>("interface"u8.ToArray()),
                    MqttQualityOfServiceLevel.AtMostOnce,
                    true,
                    TestContext.Current.CancellationToken);

                MqttZeroRetainedMessageSnapshot retained =
                    Assert.Single(concrete.GetRetainedMessageSnapshots());
                Assert.Equal("api/interface", retained.Topic);
                Assert.Equal(9, retained.PayloadLength);
            }
            finally
            {
                await concrete.DisposeAsync();
            }
        }

        [Fact]
        public void AddMqttZeroRegistersDefaultInfrastructure()
        {
            ServiceCollection services = new();
            services.AddMqttZero();
            using ServiceProvider provider = services.BuildServiceProvider();

            Assert.IsType<MqttZeroNoopSessionStore>(
                provider.GetRequiredService<IMqttZeroSessionStore>());
            Assert.IsType<MqttZeroNoopMessageStore>(
                provider.GetRequiredService<IMqttZeroMessageStore>());
            Assert.IsType<MqttZeroNoopRetainedMessageStore>(
                provider.GetRequiredService<IMqttZeroRetainedMessageStore>());
            Assert.Same(
                TimeProvider.System,
                provider.GetRequiredService<TimeProvider>());
        }
    }
}
