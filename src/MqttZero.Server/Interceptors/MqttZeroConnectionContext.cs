using MqttZero.Server.Clients;
using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Server.Interceptors;

/// <summary>
/// CONNECT 认证数据。
/// </summary>
public sealed class MqttZeroConnectionContext
{
    /// <summary>获取解析后的客户端信息。</summary>
    public required MqttZeroClientConnectionInfo Client { get; init; }

    /// <summary>获取不可变的 CONNECT 请求快照。</summary>
    public required MqttZeroConnectRequest Request { get; init; }

    /// <summary>获取原始密码序列。</summary>
    public ReadOnlySequence<byte> Password => Request.Password;
    /// <summary>获取或设置拒绝原因码。</summary>
    public MqttReasonCode RejectReasonCode { get; set; } = MqttReasonCode.NotAuthorized;
    /// <summary>获取或设置可选的拒绝原因字符串。</summary>
    public string? ReasonString { get; set; }
}
