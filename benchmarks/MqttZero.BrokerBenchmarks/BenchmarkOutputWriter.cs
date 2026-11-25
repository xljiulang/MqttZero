using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace MqttZero.BrokerBenchmarks;

internal static class BenchmarkOutputWriter
{
    public static async ValueTask WriteAsync<T>(
        string? path,
        T value,
        JsonTypeInfo<T> typeInfo,
        CancellationToken cancellationToken)
    {
        string json = JsonSerializer.Serialize(value, typeInfo);
        if (string.IsNullOrWhiteSpace(path))
        {
            Console.WriteLine(json);
            return;
        }

        string? directory = Path.GetDirectoryName(
            Path.GetFullPath(path));
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await File.WriteAllTextAsync(
            path,
            json,
            new UTF8Encoding(encoderShouldEmitUTF8Identifier: false),
            cancellationToken).ConfigureAwait(false);
    }
}
