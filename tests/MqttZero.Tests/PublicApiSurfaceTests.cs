using MqttZero.Protocol;
using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Server.Interceptors;
using System.Reflection;

namespace MqttZero.Tests;

public sealed class PublicApiSurfaceTests
{
    [Theory]
    [InlineData(typeof(MqttPacketCodec))]
    [InlineData(typeof(MqttPacket))]
    [InlineData(typeof(MqttConnectPacket))]
    [InlineData(typeof(MqttPublishPacket))]
    [InlineData(typeof(MqttPacketType))]
    public void ProtocolInternalsAreNotPublic(Type type)
    {
        Assert.False(type.IsPublic || type.IsNestedPublic);
    }

    [Theory]
    [InlineData(typeof(MqttQualityOfServiceLevel))]
    [InlineData(typeof(MqttReasonCode))]
    [InlineData(typeof(MqttPublishProperties))]
    [InlineData(typeof(MqttUserProperty))]
    [InlineData(typeof(MqttSubscriptionRequest))]
    [InlineData(typeof(MqttRetainHandling))]
    [InlineData(typeof(MqttProtocolVersion))]
    [InlineData(typeof(MqttPayloadFormatIndicator))]
    public void ProtocolContractsStayPublic(Type type)
    {
        Assert.True(type.IsPublic);
    }

    [Fact]
    public void PublicBrokerContractsDoNotExposeInternalTypes()
    {
        AssertNoInternalTypes(typeof(IMqttZeroBroker));
        AssertNoInternalTypes(typeof(MqttZeroConnectionContext));
        AssertNoInternalTypes(typeof(MqttZeroConnectRequest));
        AssertNoInternalTypes(typeof(MqttZeroWillRequest));
        AssertNoInternalTypes(typeof(MqttZeroConnectedClient));
    }

    private static void AssertNoInternalTypes(Type contract)
    {
        List<string> offenders = [];
        foreach (MemberInfo member in contract.GetMembers(
            BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static))
        {
            foreach (Type referenced in GetReferencedTypes(member))
            {
                if (!IsPublicType(referenced))
                {
                    offenders.Add($"{contract.Name}.{member.Name}: {referenced.Name}");
                }
            }
        }

        Assert.True(
            offenders.Count == 0,
            string.Join(Environment.NewLine, offenders));
    }

    private static IEnumerable<Type> GetReferencedTypes(MemberInfo member)
    {
        switch (member)
        {
            case MethodInfo method:
                yield return method.ReturnType;
                foreach (ParameterInfo parameter in method.GetParameters())
                {
                    yield return parameter.ParameterType;
                }

                break;
            case PropertyInfo property:
                yield return property.PropertyType;

                break;
        }
    }

    private static bool IsPublicType(Type type)
    {
        if (type.IsArray)
        {
            return IsPublicType(type.GetElementType()!);
        }

        if (type.IsGenericTypeDefinition)
        {
            return type.IsPublic || type.IsNestedPublic;
        }

        if (type.IsGenericType)
        {
            Type definition = type.GetGenericTypeDefinition();
            if (!IsPublicType(definition))
            {
                return false;
            }

            foreach (Type argument in type.GetGenericArguments())
            {
                if (!IsPublicType(argument))
                {
                    return false;
                }
            }

            return true;
        }

        if (type.IsByRef || type.IsPointer)
        {
            return IsPublicType(type.GetElementType()!);
        }

        return type.IsPublic ||
            type.IsNestedPublic ||
            type == typeof(void) ||
            type.IsPrimitive;
    }
}
