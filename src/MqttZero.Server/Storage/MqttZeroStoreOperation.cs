namespace MqttZero.Server.Storage;

/// <summary>
/// 表示一次持久化 Store 调用，失败时由 Broker 记录日志。
/// </summary>
/// <typeparam name="TState">The call state type.</typeparam>
/// <param name="state">The provider-supplied call state.</param>
/// <param name="cancellationToken">A token that cancels the operation.</param>
/// <returns>A task representing the store call.</returns>
internal delegate ValueTask MqttZeroStoreOperation<in TState>(
    TState state,
    CancellationToken cancellationToken);
