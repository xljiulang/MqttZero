using Microsoft.AspNetCore.Server.Kestrel.Https;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace MqttZero.AspNetCore;

/// <summary>
/// 监听端点的静态 TLS 配置。
/// </summary>
public sealed class MqttZeroTlsOptions
{
    /// <summary>
    /// 获取或设置 TLS 握手使用的服务端证书。
    /// </summary>
    public X509Certificate2? ServerCertificate { get; set; }

    /// <summary>
    /// 获取或设置启用的 TLS 协议版本。
    /// </summary>
    public SslProtocols SslProtocols { get; set; } = SslProtocols.None;

    /// <summary>
    /// 获取或设置客户端证书要求。
    /// </summary>
    public ClientCertificateMode ClientCertificateMode { get; set; } = ClientCertificateMode.NoCertificate;

    /// <summary>
    /// 获取或设置是否检查证书吊销。
    /// </summary>
    public bool CheckCertificateRevocation { get; set; } = true;

    /// <summary>
    /// 获取或设置 Kestrel HTTPS 选项的附加回调。
    /// </summary>
    public Action<HttpsConnectionAdapterOptions>? Configure { get; set; }

    /// <summary>
    /// 创建Kestrel选项。
    /// </summary>
    /// <returns>对应的结果。</returns>
    internal HttpsConnectionAdapterOptions CreateKestrelOptions()
    {
        HttpsConnectionAdapterOptions options = new()
        {
            ServerCertificate = ServerCertificate,
            SslProtocols = SslProtocols,
            ClientCertificateMode = ClientCertificateMode,
            CheckCertificateRevocation = CheckCertificateRevocation
        };
        Configure?.Invoke(options);
        if (options.ServerCertificate == null &&
            options.ServerCertificateSelector == null)
        {
            throw new InvalidOperationException(
                "A server certificate is required when MQTTZero TLS is enabled.");
        }

        return options;
    }
}
