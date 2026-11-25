using MqttZero.Server.Interceptors;

namespace MqttZero.Tests.Support;

internal sealed class ThrowingEnqueueInterceptor :
    IMqttZeroEnqueueInterceptor
{
    private readonly TaskCompletionSource _failure = new(
        TaskCreationOptions.RunContinuationsAsynchronously);

    internal Task WaitForFailureAsync()
    {
        return _failure.Task;
    }

    public ValueTask<MqttZeroEnqueueInterceptionResult>
        InterceptAsync(
            MqttZeroEnqueueContext context,
            CancellationToken cancellationToken)
    {
        _failure.TrySetResult();
        throw new InvalidOperationException("background failure");
    }
}
