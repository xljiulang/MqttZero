namespace MqttZero.Server.Storage;

/// <summary>
/// Represents one persistence store call that the broker logs on failure.
/// </summary>
/// <typeparam name="TState">The call state type.</typeparam>
/// <param name="state">The provider-supplied call state.</param>
/// <param name="cancellationToken">A token that cancels the operation.</param>
/// <returns>A task representing the store call.</returns>
internal delegate ValueTask MqttZeroStoreOperation<in TState>(
    TState state,
    CancellationToken cancellationToken);
