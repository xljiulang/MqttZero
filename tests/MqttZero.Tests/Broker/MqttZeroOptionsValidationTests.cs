using MqttZero.Server;
using MqttZero.Server.Broker;
using System.Reflection;

namespace MqttZero.Tests.Broker
{
    public sealed class MqttZeroOptionsValidationTests
    {
        [Theory]
        [InlineData(nameof(MqttZeroOptions.MaxActiveConnections))]
        [InlineData(nameof(MqttZeroOptions.MaxSessions))]
        [InlineData(nameof(MqttZeroOptions.MaxSubscriptionsPerSession))]
        [InlineData(nameof(MqttZeroOptions.MaxInflight))]
        [InlineData(nameof(MqttZeroOptions.MaxAwaitingRel))]
        [InlineData(nameof(MqttZeroOptions.MaxMessageQueueLength))]
        [InlineData(nameof(MqttZeroOptions.MaxPacketSize))]
        public void RejectsNonPositiveIntegerLimits(string propertyName)
        {
            MqttZeroOptions options = new();
            PropertyInfo property = typeof(MqttZeroOptions).GetProperty(propertyName)!;
            property.SetValue(options, 0);

            Assert.Throws<ArgumentOutOfRangeException>(
                () => MqttZeroOptionsValidator.Validate(options));
        }

        [Theory]
        [InlineData(nameof(MqttZeroOptions.MaxBufferedBytes))]
        [InlineData(nameof(MqttZeroOptions.MaxRetainedBytes))]
        public void RejectsNonPositiveLongLimits(string propertyName)
        {
            MqttZeroOptions options = new();
            PropertyInfo property = typeof(MqttZeroOptions).GetProperty(propertyName)!;
            property.SetValue(options, 0L);

            Assert.Throws<ArgumentOutOfRangeException>(
                () => MqttZeroOptionsValidator.Validate(options));
        }

        [Theory]
        [InlineData(nameof(MqttZeroOptions.ConnectTimeout))]
        [InlineData(nameof(MqttZeroOptions.IncompletePacketTimeout))]
        [InlineData(nameof(MqttZeroOptions.KeepAliveCheckInterval))]
        public void RejectsNonPositiveTimeouts(string propertyName)
        {
            MqttZeroOptions options = new();
            PropertyInfo property = typeof(MqttZeroOptions).GetProperty(propertyName)!;
            property.SetValue(options, TimeSpan.Zero);

            Assert.Throws<ArgumentOutOfRangeException>(
                () => MqttZeroOptionsValidator.Validate(options));
        }

        [Fact]
        public void RejectsNegativeSessionExpiryLimit()
        {
            MqttZeroOptions options = new()
            {
                MaxSessionExpiryInterval = TimeSpan.FromTicks(-1)
            };

            Assert.Throws<ArgumentOutOfRangeException>(
                () => MqttZeroOptionsValidator.Validate(options));
        }

        [Fact]
        public void AcceptsNullSessionExpiryLimit()
        {
            MqttZeroOptions options = new()
            {
                MaxSessionExpiryInterval = null
            };

            MqttZeroOptionsValidator.Validate(options);
        }

        [Fact]
        public void AcceptsZeroSessionExpiryLimit()
        {
            MqttZeroOptions options = new()
            {
                MaxSessionExpiryInterval = TimeSpan.Zero
            };

            MqttZeroOptionsValidator.Validate(options);
        }

        [Fact]
        public void AcceptsZeroRetainedMessageLimitAsUnlimited()
        {
            MqttZeroOptions options = new()
            {
                MaxRetainedMessages = 0
            };

            MqttZeroOptionsValidator.Validate(options);
        }

        [Theory]
        [InlineData(-1)]
        [InlineData(257)]
        public void RejectsInvalidEventLoopShardCount(int shardCount)
        {
            MqttZeroOptions options = new()
            {
                EventLoopShardCount = shardCount
            };

            Assert.Throws<ArgumentOutOfRangeException>(
                () => MqttZeroOptionsValidator.Validate(options));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(256)]
        public void AcceptsValidEventLoopShardCount(int shardCount)
        {
            MqttZeroOptions options = new()
            {
                EventLoopShardCount = shardCount
            };

            MqttZeroOptionsValidator.Validate(options);
        }

        [Fact]
        public void RejectsNegativeRetainedMessageLimit()
        {
            MqttZeroOptions options = new()
            {
                MaxRetainedMessages = -1
            };

            Assert.Throws<ArgumentOutOfRangeException>(
                () => MqttZeroOptionsValidator.Validate(options));
        }

        [Fact]
        public void RejectsInvalidResponseInformation()
        {
            MqttZeroOptions options = new()
            {
                ResponseInformation = "invalid\0response"
            };

            Assert.Throws<ArgumentException>(
                () => MqttZeroOptionsValidator.Validate(options));
        }

        [Fact]
        public void RejectsOversizedResponseInformation()
        {
            MqttZeroOptions options = new()
            {
                ResponseInformation = new string('a', ushort.MaxValue + 1)
            };

            Assert.Throws<ArgumentException>(
                () => MqttZeroOptionsValidator.Validate(options));
        }
    }
}
