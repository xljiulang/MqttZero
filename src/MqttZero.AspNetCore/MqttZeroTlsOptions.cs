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
    /// Gets or sets the server certificate used for TLS handshakes.
    /// </summary>
    public X509Certificate2? ServerCertificate { get; set; }

    /// <summary>
    /// Gets or sets the enabled TLS protocol versions.
    /// </summary>
    public SslProtocols SslProtocols { get; set; } = SslProtocols.None;

    /// <summary>
    /// Gets or sets the client certificate requirement.
    /// </summary>
    public ClientCertificateMode ClientCertificateMode { get; set; } = ClientCertificateMode.NoCertificate;

    /// <summary>
    /// Gets or sets whether certificate revocation is checked.
    /// </summary>
    public bool CheckCertificateRevocation { get; set; } = true;

    /// <summary>
    /// Gets or sets an additional callback for Kestrel HTTPS options.
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
