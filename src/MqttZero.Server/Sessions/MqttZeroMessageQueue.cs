using MqttZero.Protocol;

namespace MqttZero.Server.Sessions;

/// <summary>
/// 会话内按 QoS 分链的待发送消息队列。
/// </summary>
internal sealed class MqttZeroMessageQueue
{
    private int _count;
    private int _qos0CandidateCount;
    private MqttZeroQueuedMessage? _head;
    private MqttZeroQueuedMessage? _qos0Head;
    private MqttZeroQueuedMessage? _qos0Tail;
    private MqttZeroQueuedMessage? _tail;

    /// <summary>
    /// 初始化 MqttZeroMessageQueue 的新实例。
    /// </summary>
    /// <param name="capacity">容量。</param>
    public MqttZeroMessageQueue(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        Capacity = capacity;
    }

    /// <summary>
    /// 容量。
    /// </summary>
    public int Capacity { get; }

    /// <summary>
    /// Count 的值。
    /// </summary>
    public int Count
    {
        get
        {
            return Volatile.Read(ref _count);
        }
    }

    /// <summary>
    /// ExpiryIndexCountForTest 的值。
    /// </summary>
    internal int ExpiryIndexCountForTest
    {
        get
        {
            return _head?.ExpiresAt.HasValue == true ? 1 : 0;
        }
    }

    /// <summary>
    /// Head 的值。
    /// </summary>
    private MqttZeroQueuedMessage Head
    {
        get
        {
            return _head ??
                throw new InvalidOperationException(
                    "The MQTT message queue is empty.");
        }
    }

    /// <summary>
    /// 获取下一个。
    /// </summary>
    /// <param name="message">消息。</param>
    /// <returns>对应的结果。</returns>
    private static MqttZeroQueuedMessage GetNext(MqttZeroQueuedMessage message)
    {
        return message.QueueNext ??
            throw new InvalidOperationException(
                "The MQTT message queue link is corrupted.");
    }

    /// <summary>
    /// 按索引获取元素。
    /// </summary>
    public MqttZeroQueuedMessage this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfNegative(index);
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, _count);
            MqttZeroQueuedMessage current = Head;
            for (int currentIndex = 0; currentIndex < index; currentIndex++)
            {
                current = GetNext(current);
            }

            return current;
        }
    }

    /// <summary>
    /// 添加。
    /// </summary>
    /// <param name="message">消息。</param>
    public void Add(MqttZeroQueuedMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (_count == Capacity)
        {
            throw new InvalidOperationException("The message queue is full.");
        }

        LinkLast(message);
    }

    /// <summary>
    /// 查看。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public MqttZeroQueuedMessage Peek()
    {
        if (_head == null)
        {
            throw new InvalidOperationException("The message queue is empty.");
        }

        return _head;
    }

    /// <summary>
    /// 出队。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public MqttZeroQueuedMessage Dequeue()
    {
        MqttZeroQueuedMessage message = Peek();
        Unlink(message);
        return message;
    }

    /// <summary>
    /// 执行 Prepend 对应的处理。
    /// </summary>
    /// <param name="message">消息。</param>
    public void Prepend(MqttZeroQueuedMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (_count == Capacity)
        {
            throw new InvalidOperationException("The message queue is full.");
        }

        LinkFirst(message);
    }

    /// <summary>
    /// 淘汰OldestQos0OrOldest。
    /// </summary>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// 查找索引。
    /// </summary>
    /// <param name="predicate">谓词。</param>
    /// <returns>计算得到的数值。</returns>
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

    /// <summary>
    /// 移除At。
    /// </summary>
    /// <param name="index">索引。</param>
    public void RemoveAt(int index)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, _count);
        MqttZeroQueuedMessage current = Head;
        for (int currentIndex = 0; currentIndex < index; currentIndex++)
        {
            current = GetNext(current);
        }

        Unlink(current);
    }

    /// <summary>
    /// 清空。
    /// </summary>
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
        _qos0CandidateCount = 0;
        _qos0Head = null;
        _qos0Tail = null;
    }

    /// <summary>
    /// 移除Expired。
    /// </summary>
    /// <param name="now">当前时间。</param>
    /// <param name="expired">对应的 expired。</param>
    public void RemoveExpired(
        DateTimeOffset now,
        List<MqttZeroQueuedMessage> expired)
    {
        ArgumentNullException.ThrowIfNull(expired);
        long nowTicks = now.UtcTicks;
        while (_head != null &&
            _head.ExpiresAt.HasValue &&
            _head.ExpiresAt.Value.UtcTicks <= nowTicks)
        {
            MqttZeroQueuedMessage message = _head;
            Unlink(message);
            expired.Add(message);
        }
    }

    /// <summary>
    /// 获取Enumerator。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public Enumerator GetEnumerator()
    {
        return new Enumerator(_head);
    }

    /// <summary>
    /// Link第一个。
    /// </summary>
    /// <param name="message">消息。</param>
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

    /// <summary>
    /// Link最后一个。
    /// </summary>
    /// <param name="message">消息。</param>
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

    /// <summary>
    /// 准备把消息链接进队列。
    /// </summary>
    /// <param name="message">消息。</param>
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

    /// <summary>
    /// 完成Link。
    /// </summary>
    /// <param name="message">消息。</param>
    private void CompleteLink(MqttZeroQueuedMessage message)
    {
        _count++;
        if (message.QualityOfServiceLevel ==
            MqttQualityOfServiceLevel.AtMostOnce)
        {
            AddQos0Candidate(
                message,
                prepend: ReferenceEquals(_head, message));
        }
    }

    /// <summary>
    /// 添加Qos0Candidate。
    /// </summary>
    /// <param name="message">消息。</param>
    /// <param name="prepend">对应的 prepend。</param>
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

    /// <summary>
    /// 移除Qos0Candidate。
    /// </summary>
    /// <param name="message">消息。</param>
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

    /// <summary>
    /// 执行 Unlink 对应的处理。
    /// </summary>
    /// <param name="message">消息。</param>
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

        message.QueueNext = null;
        message.QueuePrevious = null;
        message.IsQueued = false;
        message.QueueVersion++;
        _count--;
    }

    /// <summary>
    /// 集合的只读枚举器。
    /// </summary>
    internal struct Enumerator
    {
        private MqttZeroQueuedMessage? _current;
        private MqttZeroQueuedMessage? _next;

        /// <summary>
        /// 初始化 Enumerator 的新实例。
        /// </summary>
        /// <param name="head">对应的 head。</param>
        public Enumerator(MqttZeroQueuedMessage? head)
        {
            _current = null;
            _next = head;
        }

        /// <summary>
        /// 获取当前元素；尚未调用 <see cref="MoveNext"/> 或已结束时访问会抛出异常。
        /// </summary>
        public MqttZeroQueuedMessage Current
        {
            get
            {
                return _current ?? throw new InvalidOperationException(
                    "枚举尚未开始或已经结束。");
            }
        }

        /// <summary>
        /// Move下一个。
        /// </summary>
        /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
        public bool MoveNext()
        {
            if (_next == null)
            {
                return false;
            }

            MqttZeroQueuedMessage current = _next;
            _current = current;
            _next = current.QueueNext;
            return true;
        }
    }

}
