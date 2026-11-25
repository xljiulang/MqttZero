using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Clients;
using MqttZero.Server.Egress;
using MqttZero.Server.Routing;
using MqttZero.Server.Scheduling;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Text;

namespace MqttZero.Server.Broker;

/// <summary>
/// Implements broker startup and shutdown.
/// </summary>
public sealed partial class MqttZeroBroker
{
    /// <summary>
    /// Starts the broker and maintenance scheduler.
    /// </summary>
    /// <param name="cancellationToken">A token that cancels startup.</param>
    /// <returns>A task representing startup.</returns>
    public async ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (State == MqttZeroBrokerState.Running)
            {
                return;
            }

            if (State == MqttZeroBrokerState.Stopping)
            {
                throw new InvalidOperationException("The MQTTZero broker is stopping.");
            }

            Volatile.Write(ref _fanoutFailed, 0);
            int shardCount = MqttZeroShardCountResolver.Resolve(
                _options.EventLoopShardCount);
            _sessionShardMap = new MqttZeroSessionShardMap(shardCount);
            _routeEpochs.Reset(MqttZeroRouteEpoch.Empty);
            int shardQueueCapacity = Math.Max(
                1024,
                _options.MaxMessageQueueLength / shardCount);
            MqttZeroShardSet shardSet = new(
                shardCount,
                shardQueueCapacity,
                _fanoutDiagnostics);
            MqttZeroRoutePlannerLanes? routePlannerLanes = null;
            MqttZeroRoutePlanBufferPool? qosZeroPlanBuffers = null;
            try
            {
                routePlannerLanes = new MqttZeroRoutePlannerLanes(shardCount);
                qosZeroPlanBuffers = new MqttZeroRoutePlanBufferPool(
                    shardCount);
                MqttZeroFanoutDispatcher fanoutDispatcher = new(
                    shardSet,
                    DeliverPlannedAsync,
                    HandleFanoutFailure,
                    _fanoutDiagnostics);
                _routePlannerLanes = routePlannerLanes;
                _qosZeroPlanBuffers = qosZeroPlanBuffers;
                _fanoutDispatcher = fanoutDispatcher;
                _shardSet = shardSet;
                await RestorePersistenceAsync(cancellationToken).ConfigureAwait(false);
                _connectionsDrained = CreateDrainSignal();
                Volatile.Write(ref _stopping, 0);
                Volatile.Write(ref _state, (int)MqttZeroBrokerState.Running);
                _maintenanceCancellation = new CancellationTokenSource();
                _maintenanceTask = RunMaintenanceAsync(
                    _maintenanceCancellation.Token);
            }
            catch
            {
                _fanoutDispatcher = null;
                _routePlannerLanes = null;
                _qosZeroPlanBuffers = null;
                _shardSet = null;
                _sessionShardMap = null;
                routePlannerLanes?.Dispose();
                qosZeroPlanBuffers?.Dispose();
                await shardSet.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    /// <summary>
    /// Stops the broker and closes active connections.
    /// </summary>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task representing shutdown.</returns>
    public async ValueTask StopAsync(CancellationToken cancellationToken = default)
    {
        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (State is MqttZeroBrokerState.Stopped or MqttZeroBrokerState.Created)
            {
                return;
            }

            Volatile.Write(ref _stopping, 1);
            Volatile.Write(ref _state, (int)MqttZeroBrokerState.Stopping);
            CancellationTokenSource[] acceptedConnections;
            lock (_sessionGate)
            {
                acceptedConnections = _acceptedConnections.Values.ToArray();
            }

            foreach (CancellationTokenSource accepted in acceptedConnections)
            {
                try
                {
                    await accepted.CancelAsync().ConfigureAwait(false);
                }
                catch (ObjectDisposedException)
                {
                }
            }

            if (_maintenanceCancellation != null)
            {
                await _maintenanceCancellation.CancelAsync().ConfigureAwait(false);
            }

            if (_maintenanceTask != null)
            {
                try
                {
                    await _maintenanceTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
            }

            _maintenanceCancellation?.Dispose();
            _maintenanceCancellation = null;
            _maintenanceTask = null;

            foreach (MqttZeroSessionState session in _sessions.Values)
            {
                MqttZeroClientConnection? connection;
                MqttZeroEgressActor? pump;
                lock (session.SyncRoot)
                {
                    connection = session.Connection;
                    pump = session.Egress;
                    session.Egress = null;
                }

                if (pump != null)
                {
                    connection?.Abort();
                    await StopEgressAsync(pump).ConfigureAwait(false);
                }

                if (connection == null ||
                    connection.IsClosed)
                {
                    continue;
                }

                try
                {
                    await connection.CloseAsync(MqttReasonCode.ServerShuttingDown, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (Exception) when (cancellationToken.IsCancellationRequested)
                {
                }
            }

            Task drained = _connectionsDrained.Task;
            if (Volatile.Read(ref _activeConnectionCount) > 0)
            {
                await drained.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            MqttZeroShardSet? shardSet = _shardSet;
            if (shardSet != null)
            {
                await shardSet.DisposeAsync().ConfigureAwait(false);
                _shardSet = null;
            }

            _fanoutDispatcher = null;
            MqttZeroRoutePlanBufferPool? qosZeroPlanBuffers =
                _qosZeroPlanBuffers;
            _qosZeroPlanBuffers = null;
            qosZeroPlanBuffers?.Dispose();
            MqttZeroRoutePlannerLanes? routePlannerLanes =
                _routePlannerLanes;
            _routePlannerLanes = null;
            routePlannerLanes?.Dispose();

            foreach (MqttZeroSessionState session in _sessions.Values)
            {
                MqttZeroEgressActor? pump;
                lock (session.SyncRoot)
                {
                    pump = session.Egress;
                    session.Egress = null;
                }

                await StopEgressAsync(pump).ConfigureAwait(false);
                ReleaseSessionMessages(session);
            }

            foreach (string topic in _retainedMessages.Keys)
            {
                RemoveRetainedMessage(topic);
            }

            _sessions.Clear();
            _sessionShardMap = null;
            _routeEpochs.Reset(MqttZeroRouteEpoch.Empty);
            Volatile.Write(ref _state, (int)MqttZeroBrokerState.Stopped);
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }
}
