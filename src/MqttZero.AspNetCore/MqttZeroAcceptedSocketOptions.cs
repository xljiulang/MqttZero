using System.Net;
using System.Net.Sockets;

namespace MqttZero.AspNetCore;

/// <summary>
/// Kestrel 已受理套接字的选项。
/// </summary>
public sealed class MqttZeroAcceptedSocketOptions
{
    /// <summary>
    /// 获取或设置 TCP no-delay 是否启用；为空时沿用宿主设置。
    /// </summary>
    public bool? NoDelay { get; set; }

    /// <summary>
    /// 获取或设置 TCP 保活是否启用；为空时沿用宿主设置。
    /// </summary>
    public bool? KeepAlive { get; set; }

    /// <summary>
    /// 获取或设置 TCP 保活空闲时间。
    /// </summary>
    public TimeSpan? KeepAliveTime { get; set; }

    /// <summary>
    /// 获取或设置 TCP 保活探测间隔。
    /// </summary>
    public TimeSpan? KeepAliveInterval { get; set; }

    /// <summary>
    /// 获取或设置 TCP 保活探测次数。
    /// </summary>
    public int? KeepAliveRetryCount { get; set; }

    /// <summary>
    /// 获取或设置已受理套接字的 linger 状态。
    /// </summary>
    public LingerOption? LingerState { get; set; }

    /// <summary>
    /// 获取或设置 IPv6 已受理套接字是否启用双栈模式。
    /// </summary>
    public bool? DualMode { get; set; }

    /// <summary>
    /// 获取或设置已受理套接字是否允许地址复用。
    /// </summary>
    public bool? AcceptedReuseAddress { get; set; }

    /// <summary>
    /// 获取或设置在应用标准选项之后执行的回调。
    /// </summary>
    public Action<Socket, EndPoint?>? Configure { get; set; }

    /// <summary>
    /// 应用。
    /// </summary>
    /// <param name="socket">对应的 socket。</param>
    /// <param name="remoteEndPoint">远端EndPoint。</param>
    internal void Apply(Socket socket, EndPoint? remoteEndPoint)
    {
        if (socket.AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6)
        {
            if (NoDelay.HasValue)
            {
                socket.NoDelay = NoDelay.Value;
            }

            if (KeepAlive.HasValue)
            {
                socket.SetSocketOption(
                    SocketOptionLevel.Socket,
                    SocketOptionName.KeepAlive,
                    KeepAlive.Value);
            }

            ApplyKeepAliveTime(socket, KeepAliveTime);
            ApplyKeepAliveTime(socket, KeepAliveInterval, SocketOptionName.TcpKeepAliveInterval);
            if (KeepAliveRetryCount.HasValue)
            {
                socket.SetSocketOption(
                    SocketOptionLevel.Tcp,
                    SocketOptionName.TcpKeepAliveRetryCount,
                    KeepAliveRetryCount.Value);
            }

            if (LingerState != null)
            {
                socket.LingerState = LingerState;
            }

            if (DualMode.HasValue &&
                socket.AddressFamily == AddressFamily.InterNetworkV6)
            {
                socket.DualMode = DualMode.Value;
            }
        }

        if (AcceptedReuseAddress.HasValue)
        {
            socket.SetSocketOption(
                SocketOptionLevel.Socket,
                SocketOptionName.ReuseAddress,
                AcceptedReuseAddress.Value);
        }

        Configure?.Invoke(socket, remoteEndPoint);
    }

    /// <summary>
    /// 应用KeepAlive时间。
    /// </summary>
    /// <param name="socket">对应的 socket。</param>
    /// <param name="value">值。</param>
    /// <param name="optionName">option名称。</param>
    private static void ApplyKeepAliveTime(
        Socket socket,
        TimeSpan? value,
        SocketOptionName optionName = SocketOptionName.TcpKeepAliveTime)
    {
        if (!value.HasValue)
        {
            return;
        }

        if (value.Value <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(value),
                "TCP keep-alive durations must be positive.");
        }

        int seconds = checked((int)Math.Ceiling(value.Value.TotalSeconds));
        socket.SetSocketOption(SocketOptionLevel.Tcp, optionName, seconds);
    }
}
