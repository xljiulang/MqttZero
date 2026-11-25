using MqttZero.Protocol;

namespace MqttZero.Server.Sessions;

internal sealed class MqttZeroMessageQueue
{
    private int _count;
    private int _expiringCount;
    private int _qos0CandidateCount;
    private MqttZeroQueuedMessage? _head;
    private MqttZeroQueuedMessage? _qos0Head;
    private MqttZeroQueuedMessage? _qos0Tail;
    private MqttZeroQueuedMessage? _tail;

    public MqttZeroMessageQueue(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        Capacity = capacity;
    }

    public int Capacity { get; }

    public int Count
    {
        get
        {
            return _count;
        }
    }

    public bool HasExpiringMessages
    {
        get
        {
            return _expiringCount > 0;
        }
    }

    public MqttZeroQueuedMessage this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfNegative(index);
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, _count);
            MqttZeroQueuedMessage current = _head!;
            for (int currentIndex = 0; currentIndex < index; currentIndex++)
            {
                current = current.QueueNext!;
            }

            return current;
        }
    }

    public void Add(MqttZeroQueuedMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (_count == Capacity)
        {
            throw new InvalidOperationException("The message queue is full.");
        }

        LinkLast(message);
    }

    public MqttZeroQueuedMessage Peek()
    {
        if (_head == null)
        {
            throw new InvalidOperationException("The message queue is empty.");
        }

        return _head;
    }

    public MqttZeroQueuedMessage Dequeue()
    {
        MqttZeroQueuedMessage message = Peek();
        Unlink(message);
        return message;
    }

    public void Prepend(MqttZeroQueuedMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (_count == Capacity)
        {
            throw new InvalidOperationException("The message queue is full.");
        }

        LinkFirst(message);
    }

    public MqttZeroQueuedMessage? EvictOldestQos0OrOldest()
    {
        if (_qos0Head != null)
        {
            MqttZeroQueuedMessage candidate = _qos0Head;
            Unlink(candidate);
            return candidate;
        }

        if (_head == null)
        {
            return null;
        }

        MqttZeroQueuedMessage oldest = _head;
        Unlink(oldest);
        return oldest;
    }

    public int FindIndex(Predicate<MqttZeroQueuedMessage> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        int index = 0;
        for (MqttZeroQueuedMessage? current = _head;
            current != null;
            current = current.QueueNext)
        {
            if (predicate(current))
            {
                return index;
            }

            index++;
        }

        return -1;
    }

    public void RemoveAt(int index)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, _count);
        MqttZeroQueuedMessage current = _head!;
        for (int currentIndex = 0; currentIndex < index; currentIndex++)
        {
            current = current.QueueNext!;
        }

        Unlink(current);
    }

    public void Clear()
    {
        MqttZeroQueuedMessage? current = _head;
        while (current != null)
        {
            MqttZeroQueuedMessage? next = current.QueueNext;
            current.QueueNext = null;
            current.QueuePrevious = null;
            current.IsQueued = false;
            current.QueueVersion++;
            RemoveQos0Candidate(current);
            current = next;
        }

        _head = null;
        _tail = null;
        _count = 0;
        _expiringCount = 0;
        _qos0CandidateCount = 0;
        _qos0Head = null;
        _qos0Tail = null;
    }

    public void RemoveExpired(
        DateTimeOffset now,
        List<MqttZeroQueuedMessage> expired)
    {
        ArgumentNullException.ThrowIfNull(expired);
        if (_expiringCount == 0)
        {
            return;
        }

        MqttZeroQueuedMessage? current = _head;
        while (current != null)
        {
            MqttZeroQueuedMessage? next = current.QueueNext;
            if (current.IsExpired(now))
            {
                Unlink(current);
                expired.Add(current);
            }

            current = next;
        }
    }

    public Enumerator GetEnumerator()
    {
        return new Enumerator(_head);
    }

    private void LinkFirst(MqttZeroQueuedMessage message)
    {
        PrepareForLink(message);
        message.QueueNext = _head;
        if (_head != null)
        {
            _head.QueuePrevious = message;
        }
        else
        {
            _tail = message;
        }

        _head = message;
        CompleteLink(message);
    }

    private void LinkLast(MqttZeroQueuedMessage message)
    {
        PrepareForLink(message);
        message.QueuePrevious = _tail;
        if (_tail != null)
        {
            _tail.QueueNext = message;
        }
        else
        {
            _head = message;
        }

        _tail = message;
        CompleteLink(message);
    }

    private static void PrepareForLink(MqttZeroQueuedMessage message)
    {
        if (message.IsQueued)
        {
            throw new InvalidOperationException(
                "The message is already linked to a queue.");
        }

        message.QueueVersion++;
        message.IsQueued = true;
    }

    private void CompleteLink(MqttZeroQueuedMessage message)
    {
        _count++;
        if (message.ExpiresAt.HasValue)
        {
            _expiringCount++;
        }

        if (message.QualityOfServiceLevel ==
            MqttQualityOfServiceLevel.AtMostOnce)
        {
            AddQos0Candidate(
                message,
                prepend: ReferenceEquals(_head, message));
        }
    }

    private void AddQos0Candidate(
        MqttZeroQueuedMessage message,
        bool prepend)
    {
        message.IsQos0Queued = true;
        if (prepend)
        {
            message.Qos0Previous = null;
            message.Qos0Next = _qos0Head;
            if (_qos0Head != null)
            {
                _qos0Head.Qos0Previous = message;
            }
            else
            {
                _qos0Tail = message;
            }

            _qos0Head = message;
        }
        else
        {
            message.Qos0Previous = _qos0Tail;
            message.Qos0Next = null;
            if (_qos0Tail != null)
            {
                _qos0Tail.Qos0Next = message;
            }
            else
            {
                _qos0Head = message;
            }

            _qos0Tail = message;
        }

        _qos0CandidateCount++;
    }

    private void RemoveQos0Candidate(MqttZeroQueuedMessage message)
    {
        if (!message.IsQos0Queued)
        {
            return;
        }

        MqttZeroQueuedMessage? previous = message.Qos0Previous;
        MqttZeroQueuedMessage? next = message.Qos0Next;
        if (previous != null)
        {
            previous.Qos0Next = next;
        }
        else
        {
            _qos0Head = next;
        }

        if (next != null)
        {
            next.Qos0Previous = previous;
        }
        else
        {
            _qos0Tail = previous;
        }

        message.Qos0Next = null;
        message.Qos0Previous = null;
        message.IsQos0Queued = false;
        _qos0CandidateCount--;
    }

    private void Unlink(MqttZeroQueuedMessage message)
    {
        RemoveQos0Candidate(message);
        MqttZeroQueuedMessage? previous = message.QueuePrevious;
        MqttZeroQueuedMessage? next = message.QueueNext;
        if (previous != null)
        {
            previous.QueueNext = next;
        }
        else
        {
            _head = next;
        }

        if (next != null)
        {
            next.QueuePrevious = previous;
        }
        else
        {
            _tail = previous;
        }

        if (message.ExpiresAt.HasValue)
        {
            _expiringCount--;
        }

        message.QueueNext = null;
        message.QueuePrevious = null;
        message.IsQueued = false;
        message.QueueVersion++;
        _count--;
    }

    internal struct Enumerator
    {
        private MqttZeroQueuedMessage? _next;

        public Enumerator(MqttZeroQueuedMessage? head)
        {
            _next = head;
            Current = null!;
        }

        public MqttZeroQueuedMessage Current { get; private set; }

        public bool MoveNext()
        {
            if (_next == null)
            {
                return false;
            }

            Current = _next;
            _next = Current.QueueNext;
            return true;
        }
    }

}
