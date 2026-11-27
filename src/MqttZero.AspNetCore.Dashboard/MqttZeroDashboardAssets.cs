using MqttZero.Server.Broker;
using MqttZero.Protocol;
using System.Reflection;

namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// 管理后台的静态资源清单。
/// </summary>
internal static class MqttZeroDashboardAssets
{
    private const string ResourcePrefix = "MqttZeroDashboard/";
    private static readonly Assembly DashboardAssembly = typeof(MqttZeroDashboardAssets).Assembly;

    /// <summary>
    /// 尝试Open。
    /// </summary>
    /// <param name="path">路径。</param>
    /// <param name="stream">对应的 stream。</param>
    /// <param name="contentType">content类型。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public static bool TryOpen(
        string path,
        out Stream? stream,
        out string contentType)
    {
        string normalizedPath = string.IsNullOrWhiteSpace(path) || path == "/"
            ? "index.html"
            : path.TrimStart('/');
        string resourceName = string.Concat(ResourcePrefix, normalizedPath);
        stream = DashboardAssembly.GetManifestResourceStream(resourceName);
        if (stream == null)
        {
            string windowsResourceName = string.Concat(
                ResourcePrefix,
                normalizedPath.Replace('/', Path.DirectorySeparatorChar));
            stream = DashboardAssembly.GetManifestResourceStream(windowsResourceName);
        }

        contentType = GetContentType(normalizedPath);
        return stream != null;
    }

    /// <summary>
    /// 获取Content类型。
    /// </summary>
    /// <param name="path">路径。</param>
    /// <returns>对应的字符串。</returns>
    private static string GetContentType(string path)
    {
        return Path.GetExtension(path).ToLowerInvariant() switch
        {
            ".html" => "text/html; charset=utf-8",
            ".css" => "text/css; charset=utf-8",
            ".js" => "text/javascript; charset=utf-8",
            ".json" => "application/json; charset=utf-8",
            ".svg" => "image/svg+xml",
            ".png" => "image/png",
            ".ico" => "image/x-icon",
            _ => "application/octet-stream"
        };
    }
}
