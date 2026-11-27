using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Server.Interceptors;

/// <summary>
/// 拦截器可见的 CONNECT 请求快照，替代直接暴露原始报文。
/// </summary>
public sealed class MqttZeroConnectRequest
{
    /// <summary>
    /// 获取客户端标识（原始字节）。
    /// </summary>
    public required ReadOnlySequence<byte> ClientId { get; init; }

    /// <summary>
    /// 获取协议版本。
    /// </summary>
    public MqttProtocolVersion ProtocolVersion { get; init; } = MqttProtocolVersion.V500;

    /// <summary>
    /// 获取是否要求清理会话。
    /// </summary>
    public bool CleanStart { get; init; }

    /// <summary>
    /// 获取保活秒数。
    /// </summary>
    public ushort KeepAliveSeconds { get; init; }

    /// <summary>
    /// 获取会话过期时间；未指定时为 <see langword="null"/>。
    /// </summary>
    public uint? SessionExpiryInterval { get; init; }

    /// <summary>
    /// 获取客户端声明的接收上限。
    /// </summary>
    public ushort ReceiveMaximum { get; init; } = ushort.MaxValue;

    /// <summary>
    /// 获取客户端声明的最大报文大小。
    /// </summary>
    public uint MaximumPacketSize { get; init; } = uint.MaxValue;

    /// <summary>
    /// 获取是否请求问题诊断信息。
    /// </summary>
    public bool RequestProblemInformation { get; init; } = true;

    /// <summary>
    /// 获取是否请求响应信息。
    /// </summary>
    public bool RequestResponseInformation { get; init; }

    /// <summary>
    /// 获取用户名；未提供时为 <see langword="null"/>。
    /// </summary>
    public string? UserName { get; init; }

    /// <summary>
    /// 获取密码原始字节；未提供时为空序列。
    /// </summary>
    public ReadOnlySequence<byte> Password { get; init; }

    /// <summary>
    /// 获取遗嘱消息；未设置时为 <see langword="null"/>。
    /// </summary>
    public MqttZeroWillRequest? Will { get; init; }
}
