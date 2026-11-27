using System.Collections;

namespace MqttZero.Server.Sessions;

/// <summary>
/// 会话的订阅集合。
/// </summary>
internal sealed class MqttZeroSubscriptionCollection :
    IReadOnlyList<MqttZeroSubscription>
{
    private MqttZeroSubscription[] _items = [];

    public int Count
    {
        get
        {
            return Volatile.Read(ref _items).Length;
        }
    }

    public MqttZeroSubscription this[int index]
    {
        get
        {
            return Volatile.Read(ref _items)[index];
        }
    }

    /// <summary>
    /// 添加。
    /// </summary>
    /// <param name="subscription">订阅。</param>
    public void Add(MqttZeroSubscription subscription)
    {
        ArgumentNullException.ThrowIfNull(subscription);
        while (true)
        {
            MqttZeroSubscription[] current = Volatile.Read(ref _items);
            MqttZeroSubscription[] replacement =
                new MqttZeroSubscription[current.Length + 1];
            Array.Copy(current, replacement, current.Length);
            replacement[^1] = subscription;
            if (ReferenceEquals(
                    Interlocked.CompareExchange(
                        ref _items,
                        replacement,
                        current),
                    current))
            {
                return;
            }
        }
    }

    /// <summary>
    /// 替换。
    /// </summary>
    /// <param name="subscriptions">订阅。</param>
    public void Replace(
        IReadOnlyList<MqttZeroSubscription> subscriptions)
    {
        ArgumentNullException.ThrowIfNull(subscriptions);
        MqttZeroSubscription[] replacement;
        if (subscriptions is MqttZeroSubscription[] array)
        {
            replacement = (MqttZeroSubscription[])array.Clone();
        }
        else
        {
            replacement =
                new MqttZeroSubscription[subscriptions.Count];
            for (int index = 0; index < subscriptions.Count; index++)
            {
                replacement[index] = subscriptions[index];
            }
        }

        Volatile.Write(ref _items, replacement);
    }

    /// <summary>
    /// 快照。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public MqttZeroSubscription[] Snapshot()
    {
        return Volatile.Read(ref _items);
    }

    /// <summary>
    /// 获取Enumerator。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public IEnumerator<MqttZeroSubscription> GetEnumerator()
    {
        return ((IEnumerable<MqttZeroSubscription>)Snapshot())
            .GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}
