using System.IO.Pipelines;
using System.Runtime.CompilerServices;

namespace MqttZero.Server.Egress;

/// <summary>
/// 单次挂起 flush 的完成跟踪。
/// </summary>
internal sealed class MqttZeroEgressFlushOperation
{
    private readonly Action<Exception?> _completed;
    private readonly Action _continuation;
    private ConfiguredValueTaskAwaitable<FlushResult>.
        ConfiguredValueTaskAwaiter _awaiter;
    private int _running;

    /// <summary>
    /// 初始化 MqttZeroEgressFlushOperation 的新实例。
    /// </summary>
    /// <param name="completed">对应的 completed。</param>
    public MqttZeroEgressFlushOperation(
        Action<Exception?> completed)
    {
        _completed = completed ??
            throw new ArgumentNullException(nameof(completed));
        _continuation = Complete;
    }

    /// <summary>
    /// 启动。
    /// </summary>
    /// <param name="flush">对应的 flush。</param>
    public void Start(ValueTask<FlushResult> flush)
    {
        if (Interlocked.CompareExchange(ref _running, 1, 0) != 0)
        {
            throw new InvalidOperationException(
                "The flush operation is already running.");
        }

        ConfiguredValueTaskAwaitable<FlushResult>.
            ConfiguredValueTaskAwaiter awaiter = flush
                .ConfigureAwait(false)
                .GetAwaiter();
        _awaiter = awaiter;
        if (awaiter.IsCompleted)
        {
            Complete();
            return;
        }

        try
        {
            awaiter.UnsafeOnCompleted(_continuation);
        }
        catch
        {
            _awaiter = default;
            Volatile.Write(ref _running, 0);
            throw;
        }
    }

    /// <summary>
    /// 完成。
    /// </summary>
    private void Complete()
    {
        Exception? failure = null;
        try
        {
            FlushResult result = _awaiter.GetResult();
            if (result.IsCompleted)
            {
                failure = new IOException(
                    "The MQTT output writer completed during flush.");
            }
        }
        catch (Exception exception)
        {
            failure = exception;
        }
        finally
        {
            _awaiter = default;
            Volatile.Write(ref _running, 0);
        }

        _completed(failure);
    }
}
