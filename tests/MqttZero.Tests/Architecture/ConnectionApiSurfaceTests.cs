using MqttZero.AspNetCore;
using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using System.IO.Pipelines;
using System.Net;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace MqttZero.Tests.Architecture;

public sealed class ConnectionApiSurfaceTests
{
    [Fact]
    public void TransportConnectionIsPublicAndPreservesValues()
    {
        Type transportType = GetRequiredType(
            "MqttZero.Server.Clients.MqttZeroTransportConnection");
        PropertyInfo inputProperty = transportType.GetProperty("Input")!;
        PropertyInfo outputProperty = transportType.GetProperty("Output")!;
        DnsEndPoint remoteEndPoint = new("broker.example", 1883);
        PipeReader input = PipeReader.Create(Stream.Null);
        PipeWriter output = PipeWriter.Create(Stream.Null);
        using RSA rsa = RSA.Create(2048);
        CertificateRequest request = new(
            "CN=mqttzero-transport-test",
            rsa,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        using X509Certificate2 certificate = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddMinutes(-1),
            DateTimeOffset.UtcNow.AddMinutes(1));
        object transport = Activator.CreateInstance(transportType)!;

        inputProperty.SetValue(transport, input);
        outputProperty.SetValue(transport, output);
        transportType.GetProperty("RemoteEndPoint")!.SetValue(transport, remoteEndPoint);
        transportType.GetProperty("IsSecure")!.SetValue(transport, true);
        transportType.GetProperty("IsWebSocket")!.SetValue(transport, true);
        transportType.GetProperty("ClientCertificate")!.SetValue(
            transport,
            certificate);

        Assert.True(transportType.IsPublic);
        Assert.True(transportType.IsSealed);
        AssertRequiredInitOnly(inputProperty);
        AssertRequiredInitOnly(outputProperty);
        Assert.Same(input, inputProperty.GetValue(transport));
        Assert.Same(output, outputProperty.GetValue(transport));
        Assert.Same(
            remoteEndPoint,
            transportType.GetProperty("RemoteEndPoint")!.GetValue(transport));
        Assert.True((bool)transportType.GetProperty("IsSecure")!.GetValue(transport)!);
        Assert.True((bool)transportType.GetProperty("IsWebSocket")!.GetValue(transport)!);
        Assert.Same(
            certificate,
            transportType.GetProperty("ClientCertificate")!.GetValue(transport));
        Assert.True((bool)transportType.GetProperty(
            "AllowPacketFragmentation")!.GetValue(transport)!);
    }

    [Fact]
    public void BrokerExposesOnlyTransportConnectionAcceptance()
    {
        Type transportType = GetRequiredType(
            "MqttZero.Server.Clients.MqttZeroTransportConnection");
        MethodInfo method = Assert.Single(
            typeof(MqttZeroBroker)
                .GetMethods(BindingFlags.Instance | BindingFlags.Public),
            static candidate =>
                candidate.Name == nameof(MqttZeroBroker.AcceptConnectionAsync));
        ParameterInfo[] parameters = method.GetParameters();

        Assert.Equal(2, parameters.Length);
        Assert.Equal(transportType, parameters[0].ParameterType);
        Assert.Equal(typeof(CancellationToken), parameters[1].ParameterType);
        Assert.True(parameters[1].HasDefaultValue);
    }

    [Fact]
    public async Task BrokerRejectsMissingTransportPipes()
    {
        await using MqttZeroBroker broker = MqttZeroBroker.Create(
            new MqttZeroOptions(),
            TimeProvider.System);
        MqttZeroTransportConnection missingInput = new()
        {
            Input = null!,
            Output = PipeWriter.Create(Stream.Null)
        };
        MqttZeroTransportConnection missingOutput = new()
        {
            Input = PipeReader.Create(Stream.Null),
            Output = null!
        };

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => broker.AcceptConnectionAsync(
                missingInput,
                TestContext.Current.CancellationToken).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => broker.AcceptConnectionAsync(
                missingOutput,
                TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public void ConnectionApiCleanupMatchesTheApprovedSurface()
    {
        const BindingFlags PublicInstance =
            BindingFlags.Instance | BindingFlags.Public;
        Assembly serverAssembly = typeof(MqttZeroBroker).Assembly;

        Assert.Null(typeof(MqttZeroBroker).GetProperty("TimeProvider", PublicInstance));
        Assert.NotNull(serverAssembly.GetType(
            "MqttZero.Server.Broker.MqttZeroBrokerExtensions",
            throwOnError: false));
        Assert.Null(typeof(MqttZeroConnectedClient).GetMethod(
            "ResetStatistics",
            PublicInstance));
        Assert.DoesNotContain(
            typeof(MqttZeroBuilderInterceptorExtensions).GetMethods(
                BindingFlags.Public | BindingFlags.Static),
            static method => method.Name == "AddPublishInterceptor");
        Assert.Null(serverAssembly.GetType(
            "MqttZero.Server.Routing.MqttZeroTopicMatcher",
            throwOnError: false));
    }

    private static Type GetRequiredType(string typeName)
    {
        Type? type = typeof(MqttZeroBroker).Assembly.GetType(
            typeName,
            throwOnError: false);
        Assert.NotNull(type);
        return type;
    }

    private static void AssertRequiredInitOnly(PropertyInfo property)
    {
        Assert.NotNull(property.GetCustomAttribute<RequiredMemberAttribute>());
        Assert.NotNull(property.SetMethod);
        Assert.Contains(
            property.SetMethod.ReturnParameter.GetRequiredCustomModifiers(),
            static modifier => modifier == typeof(IsExternalInit));
    }
}
