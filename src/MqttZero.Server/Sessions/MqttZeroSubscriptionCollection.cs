using System.Collections;

namespace MqttZero.Server.Sessions;

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

    public MqttZeroSubscription[] Snapshot()
    {
        return Volatile.Read(ref _items);
    }

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
