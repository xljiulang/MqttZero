using System.Net;
using System.Net.Sockets;

namespace MqttZero.AspNetCore;

/// <summary>
/// Kestrel 已受理套接字的选项。
/// </summary>
public sealed class MqttZeroAcceptedSocketOptions
{
    /// <summary>
    /// Gets or sets whether TCP no-delay is enabled. A null value preserves the host setting.
    /// </summary>
    public bool? NoDelay { get; set; }

    /// <summary>
    /// Gets or sets whether TCP keep-alive is enabled. A null value preserves the host setting.
    /// </summary>
    public bool? KeepAlive { get; set; }

    /// <summary>
    /// Gets or sets the TCP keep-alive idle time.
    /// </summary>
    public TimeSpan? KeepAliveTime { get; set; }

    /// <summary>
    /// Gets or sets the TCP keep-alive probe interval.
    /// </summary>
    public TimeSpan? KeepAliveInterval { get; set; }

    /// <summary>
    /// Gets or sets the number of TCP keep-alive probes.
    /// </summary>
    public int? KeepAliveRetryCount { get; set; }

    /// <summary>
    /// Gets or sets the accepted socket linger state.
    /// </summary>
    public LingerOption? LingerState { get; set; }

    /// <summary>
    /// Gets or sets whether an IPv6 accepted socket operates in dual mode.
    /// </summary>
    public bool? DualMode { get; set; }

    /// <summary>
    /// Gets or sets whether address reuse is enabled on accepted sockets.
    /// </summary>
    public bool? AcceptedReuseAddress { get; set; }

    /// <summary>
    /// Gets or sets an additional callback that runs after the standard options are applied.
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
