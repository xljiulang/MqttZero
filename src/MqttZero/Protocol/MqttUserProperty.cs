using System.Text;

namespace MqttZero.Protocol;

/// <summary>
/// 有序的用户属性名值对。
/// </summary>
public sealed class MqttUserProperty
{
    /// <summary>
    /// Initializes a new MQTT user property.
    /// </summary>
    /// <param name="name">The UTF-8 property name.</param>
    /// <param name="value">The UTF-8 property value.</param>
    /// <exception cref="ArgumentException">
    /// Thrown when a value is not valid MQTT UTF-8 or exceeds the protocol length limit.
    /// </exception>
    public MqttUserProperty(string name, string value)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(value);
        ValidateUtf8String(name, nameof(name));
        ValidateUtf8String(value, nameof(value));
        Name = name;
        Value = value;
    }

    /// <summary>
    /// Gets the property name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the property value.
    /// </summary>
    public string Value { get; }

    /// <summary>
    /// 校验UTF-8字符串。
    /// </summary>
    /// <param name="value">值。</param>
    /// <param name="parameterName">parameter名称。</param>
    private static void ValidateUtf8String(string value, string parameterName)
    {
        if (value.Contains('\0', StringComparison.Ordinal))
        {
            throw new ArgumentException("MQTT UTF-8 strings cannot contain NUL.", parameterName);
        }

        for (int index = 0; index < value.Length; index++)
        {
            if (char.IsSurrogate(value[index]) &&
                (index + 1 >= value.Length ||
                    !char.IsSurrogatePair(value[index], value[index + 1])))
            {
                throw new ArgumentException(
                    "MQTT UTF-8 strings must contain valid Unicode.",
                    parameterName);
            }

            if (char.IsHighSurrogate(value[index]))
            {
                index++;
            }
        }

        if (Encoding.UTF8.GetByteCount(value) > ushort.MaxValue)
        {
            throw new ArgumentException(
                "MQTT UTF-8 strings cannot exceed 65535 bytes.",
                parameterName);
        }
    }
}
