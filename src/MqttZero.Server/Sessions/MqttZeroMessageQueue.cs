using MqttZero.Protocol;

namespace MqttZero.Server.Sessions;

internal sealed class MqttZeroMessageQueue
{
    private readonly Qos0Candidate[] _qos0Candidates;
    private int _count;
    private int _expiringCount;
    private int _qos0CandidateCount;
    private int _qos0CandidateHead;
    private MqttZeroQueuedMessage? _head;
    private MqttZeroQueuedMessage? _tail;

    public MqttZeroMessageQueue(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        Capacity = capacity;
        _qos0Candidates = new Qos0Candidate[capacity];
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
        while (_qos0CandidateCount > 0)
        {
            Qos0Candidate candidate =
                _qos0Candidates[_qos0CandidateHead];
            _qos0Candidates[_qos0CandidateHead] = default;
            _qos0CandidateHead =
                (_qos0CandidateHead + 1) % _qos0Candidates.Length;
            _qos0CandidateCount--;
            if (candidate.Version == candidate.Message.QueueVersion &&
                candidate.Message.QualityOfServiceLevel ==
                    MqttQualityOfServiceLevel.AtMostOnce)
            {
                Unlink(candidate.Message);
                return candidate.Message;
            }
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
            current.QueueVersion++;
            current = next;
        }

        _head = null;
        _tail = null;
        _count = 0;
        _expiringCount = 0;
        _qos0CandidateHead = 0;
        _qos0CandidateCount = 0;
        Array.Clear(_qos0Candidates);
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
        if (message.QueueNext != null ||
            message.QueuePrevious != null)
        {
            throw new InvalidOperationException(
                "The message is already linked to a queue.");
        }

        message.QueueVersion++;
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
            AddQos0Candidate(message);
        }
    }

    private void AddQos0Candidate(MqttZeroQueuedMessage message)
    {
        if (_qos0CandidateCount == _qos0Candidates.Length)
        {
            CompactQos0Candidates();
            if (_qos0CandidateCount == _qos0Candidates.Length)
            {
                throw new InvalidOperationException(
                    "The QoS 0 candidate queue is full.");
            }
        }

        int index =
            (_qos0CandidateHead + _qos0CandidateCount) %
            _qos0Candidates.Length;
        _qos0Candidates[index] = new Qos0Candidate(
            message,
            message.QueueVersion);
        _qos0CandidateCount++;
    }

    private void CompactQos0Candidates()
    {
        int write = 0;
        for (int index = 0; index < _qos0CandidateCount; index++)
        {
            int sourceIndex =
                (_qos0CandidateHead + index) %
                _qos0Candidates.Length;
            Qos0Candidate candidate = _qos0Candidates[sourceIndex];
            if (candidate.Version != candidate.Message.QueueVersion)
            {
                continue;
            }

            _qos0Candidates[write++] = candidate;
        }

        Array.Clear(
            _qos0Candidates,
            write,
            _qos0Candidates.Length - write);
        _qos0CandidateHead = 0;
        _qos0CandidateCount = write;
    }

    private void Unlink(MqttZeroQueuedMessage message)
    {
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

    private readonly struct Qos0Candidate
    {
        public Qos0Candidate(
            MqttZeroQueuedMessage message,
            long version)
        {
            Message = message;
            Version = version;
        }

        public MqttZeroQueuedMessage Message { get; }

        public long Version { get; }
    }
}
