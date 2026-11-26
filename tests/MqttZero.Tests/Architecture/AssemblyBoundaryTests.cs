using MqttZero.Server;
namespace MqttZero.Tests.Architecture
{
    public sealed class AssemblyBoundaryTests
    {
        [Fact]
        public void CommonTypesArePublishedFromSharedAssembly()
        {
            AssertAssembly("MqttZero.Publishing.MqttZeroPublishMessage, MqttZero", "MqttZero");
            AssertAssembly("MqttZero.MqttZeroPayloadOwner, MqttZero", "MqttZero");
        }

        [Fact]
        public void ServerTypesArePublishedFromServerAssembly()
        {
            AssertAssembly("MqttZero.Server.MqttZeroOptions, MqttZero.Server", "MqttZero.Server");
            AssertAssembly(
                "MqttZero.Server.Broker.MqttZeroBroker, MqttZero.Server",
                "MqttZero.Server");
        }

        [Fact]
        public void ServerObservationTypesUseOwningNamespaces()
        {
            AssertTypeName(
                "MqttZero.Server.Clients.MqttZeroClientConnectionInfo, MqttZero.Server");
            AssertTypeName(
                "MqttZero.Server.Observers.MqttZeroDisconnectType, MqttZero.Server");
            AssertTypeName(
                "MqttZero.Server.Observers.MqttZeroMessageDropReason, MqttZero.Server");
        }

        [Fact]
        public void ServerSnapshotsAndConnectionsUseOwningNamespaces()
        {
            AssertTypeName(
                "MqttZero.Server.Clients.MqttZeroClientSnapshot, MqttZero.Server");
            AssertTypeName(
                "MqttZero.Server.Clients.MqttZeroClientConnection, MqttZero.Server");
            AssertTypeName(
                "MqttZero.Server.Sessions.MqttZeroSessionSnapshot, MqttZero.Server");
            AssertTypeName(
                "MqttZero.Server.Retained.MqttZeroRetainedMessageSnapshot, MqttZero.Server");
            AssertTypeName(
                "MqttZero.Server.Retained.MqttZeroRetainedMessage, MqttZero.Server");
        }

        private static Type AssertTypeName(string assemblyQualifiedTypeName)
        {
            Type? type = Type.GetType(assemblyQualifiedTypeName, false);
            Assert.NotNull(type);
            return type;
        }

        private static void AssertAssembly(string assemblyQualifiedTypeName, string expectedAssembly)
        {
            Type? type = Type.GetType(assemblyQualifiedTypeName, false);
            Assert.NotNull(type);
            Assert.Equal(expectedAssembly, type.Assembly.GetName().Name);
        }
    }
}
