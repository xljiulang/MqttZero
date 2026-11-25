# Phase 4 Route Epoch and Sharded Fanout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace full route snapshot rebuilds and per-subscriber fanout with immutable Route Epochs, path-level Copy-on-Write tries, persistent HAMT entry maps and one delivery batch per target Shard.

**Architecture:** Subscription changes build a new immutable `MqttZeroRouteEpoch` and publish it with a single volatile reference swap only after persistence succeeds. Publishers traverse one route epoch, deduplicate by Session while merging QoS and Subscription Identifiers, then dispatch at most one pooled Work Item per target Shard; the current publisher Shard executes its own batch inline.

**Tech Stack:** .NET 10, MQTT 5, Phase 1 Event Loop Shards, Phase 2 Egress Actors, persistent hash-array-mapped tries, `ArrayPool<T>`, `ConcurrentDictionary<TKey,TValue>`, xUnit v3, Microsoft Testing Platform, PowerShell 7.

**Spec:** `docs/sharded-actor-broker-plan.md` Phase 4.

## Global Constraints

- Keep all existing MQTT 5 subscription semantics: regular subscriptions, shared subscriptions, `No Local`, `Retain As Published`, Subscription Identifiers, wildcard matching and trailing empty topic levels.
- Publish paths read only the current Epoch and never rebuild or mutate routing state.
- New Epochs are published only after subscription persistence succeeds.
- Every update copies only the affected Trie path and HAMT path; unrelated Epoch subtrees remain reference-equal.
- A shared-group cursor is keyed by shared group name plus match filter and survives Epoch replacement.
- Route planning deduplicates by Session, selects the highest matching QoS, ANDs the publisher QoS later, and merges all matching Subscription Identifiers.
- One MQTT publish may enqueue at most one fanout Work Item per non-current target Shard. The publisher Shard owns its local batch inline.
- Target Shard FIFO order is the per-Session delivery order. A single publisher must not overtake its earlier publish to the same Session.
- Message leases are retained for the complete fanout operation and released exactly once.
- Retained subscription delivery uses a retained-topic trie, never a full retained dictionary scan.
- No publish-path `Encoding.UTF8.GetString`, per-message direct `ConcurrentQueue` route scope, or full snapshot rebuild.
- C# files use CRLF; one top-level type per file; public types and methods have XML comments; all `if` and method bodies use braces.
- Hot routing and planning paths allocate `0 B` after warmup. Temporary arrays use `ArrayPool<T>`.
- Use PowerShell 7 and `rg` for all commands and searches.
- Keep the existing Phase 2 Shard Egress, nonblocking Flush and Kestrel defaults unchanged.
- Development performance gate: every current matrix cell must remain at or above Phase 0 throughput `95%` and at or below Phase 0 p99 `105%`; the final MQTTnet strict gate remains Phase 8.

---

## File Structure

- `src/MqttZero.Server/Routing/`
  - Immutable route keys, HAMT nodes, Trie nodes, Epoch, update manager, planner, plan batches and retained trie.
- `src/MqttZero.Server/Broker/`
  - Epoch lifecycle, subscription mutation transaction, fanout dispatcher and broker integration.
- `src/MqttZero.Server/Scheduling/`
  - No new scheduler type is required; fanout uses the existing bounded Event Loop.
- `tests/MqttZero.Tests/Routing/`
  - HAMT, Trie, Epoch, planner, shared cursor and allocation tests.
- `tests/MqttZero.Tests/Broker/`
  - Subscription persistence ordering, Shard fanout, ordering and lease-lifetime integration tests.
- `benchmarks/MqttZero.Benchmarks/`
  - Epoch lookup, 100/1000 subscriber planning and retained matching benchmarks.
- `perf/baseline/`
  - Phase 4 comparison and report.

---

### Task 1: Persistent HAMT Foundation

**Files:**

- Create: `src/MqttZero.Server/Routing/MqttZeroPersistentHashMap.cs`
- Create: `src/MqttZero.Server/Routing/MqttZeroPersistentHashMapEntry.cs`
- Create: `src/MqttZero.Server/Routing/MqttZeroPersistentHashMapNode.cs`
- Create: `src/MqttZero.Server/Routing/MqttZeroPersistentHashMapBitmapNode.cs`
- Create: `src/MqttZero.Server/Routing/MqttZeroPersistentHashMapCollisionNode.cs`
- Create: `src/MqttZero.Server/Routing/MqttZeroPersistentHashMapLeafNode.cs`
- Create: `tests/MqttZero.Tests/Routing/PersistentHashMapTests.cs`

**Interfaces:**

- Produces: `MqttZeroPersistentHashMap<TKey,TValue>` with:
  - `MqttZeroPersistentHashMap(IEqualityComparer<TKey> comparer)`
  - `static MqttZeroPersistentHashMap<TKey,TValue> Create(IEqualityComparer<TKey> comparer)`
  - `int Count`
  - `bool TryGetValue(TKey key, out TValue value)`
  - `MqttZeroPersistentHashMap<TKey,TValue> Set(TKey key, TValue value, out bool added)`
  - `MqttZeroPersistentHashMap<TKey,TValue> Remove(TKey key, out bool removed)`
  - `Enumerator GetEnumerator()`
- `Enumerator` is a `ref struct`; it yields entries without allocating and supports `MoveNext`, `Current`.
- Hash uses the supplied comparer's `GetHashCode`; equality uses the supplied comparer.

- [x] **Step 1: Write failing structural-sharing and enumeration tests**

```csharp
[Fact]
public void SetAndRemovePreserveUnchangedSubtrees()
{
    MqttZeroPersistentHashMap<string, int> empty =
        MqttZeroPersistentHashMap<string, int>.Create(
            StringComparer.Ordinal);
    MqttZeroPersistentHashMap<string, int> first =
        empty.Set("a", 1, out bool addedA);
    MqttZeroPersistentHashMap<string, int> second =
        first.Set("b", 2, out bool addedB);
    MqttZeroPersistentHashMap<string, int> removed =
        second.Remove("a", out bool removedA);

    Assert.True(addedA);
    Assert.True(addedB);
    Assert.True(removedA);
    Assert.Equal(2, second.Count);
    Assert.Equal(1, removed.Count);
    Assert.True(second.TryGetValue("a", out int value));
    Assert.Equal(1, value);
    Assert.False(removed.TryGetValue("a", out _));
}
```

```csharp
[Fact]
public void EnumeratorReturnsEveryEntryAfterHashCollisions()
{
    MqttZeroPersistentHashMap<CollisionKey, int> map =
        MqttZeroPersistentHashMap<CollisionKey, int>.Create(
            CollisionKeyComparer.Instance);
    for (int index = 0; index < 100; index++)
    {
        map = map.Set(new CollisionKey(index), index, out _);
    }

    HashSet<int> values = [];
    foreach (MqttZeroPersistentHashMapEntry<CollisionKey, int> entry in map)
    {
        values.Add(entry.Value);
    }

    Assert.Equal(Enumerable.Range(0, 100), values.Order());
}

private readonly record struct CollisionKey(int Value);

private sealed class CollisionKeyComparer :
    IEqualityComparer<CollisionKey>
{
    public static CollisionKeyComparer Instance { get; } = new();

    public bool Equals(CollisionKey x, CollisionKey y)
    {
        return x.Value == y.Value;
    }

    public int GetHashCode(CollisionKey obj)
    {
        return 1;
    }
}
```

- [x] **Step 2: Run the tests and verify RED**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~PersistentHashMapTests"
```

Expected: compilation fails because `MqttZeroPersistentHashMap<TKey,TValue>` does not exist.

- [x] **Step 3: Implement the persistent HAMT**

Use 5 hash bits per level and 32-way bitmap nodes. A bitmap node stores only non-empty children in a compact array. When two keys collide at the current fragment, create a new bitmap node at the next shift. When all 32 bits are exhausted, use a collision node. `Set` and `Remove` rebuild only nodes on the key path.

The immutable node contract is:

```csharp
internal abstract class MqttZeroPersistentHashMapNode<TKey, TValue>
{
    public abstract int Count { get; }

    public abstract bool TryGetValue(
        TKey key,
        int hash,
        uint shift,
        IEqualityComparer<TKey> comparer,
        out TValue value);

    public abstract MqttZeroPersistentHashMapNode<TKey, TValue> Set(
        TKey key,
        TValue value,
        int hash,
        uint shift,
        IEqualityComparer<TKey> comparer,
        ref bool added);

    public abstract MqttZeroPersistentHashMapNode<TKey, TValue>? Remove(
        TKey key,
        int hash,
        uint shift,
        IEqualityComparer<TKey> comparer,
        ref bool removed);
}
```

- [x] **Step 4: Add allocation and reference-sharing tests**

```csharp
[Fact]
public void ReadsAndEnumerationDoNotAllocateAfterWarmup()
{
    MqttZeroPersistentHashMap<string, int> map =
        MqttZeroPersistentHashMap<string, int>.Create(
            StringComparer.Ordinal);
    for (int index = 0; index < 100; index++)
    {
        map = map.Set(index.ToString(), index, out _);
    }

    for (int index = 0; index < 10; index++)
    {
        Assert.True(map.TryGetValue("50", out _));
    }

    long before = GC.GetAllocatedBytesForCurrentThread();
    int sum = 0;
    for (int index = 0; index < 1_000; index++)
    {
        _ = map.TryGetValue("50", out int value);
        sum += value;
    }

    Assert.Equal(50_000, sum);
    Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);
}
```

- [x] **Step 5: Run tests**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~PersistentHashMapTests"
perf/verify-allocation.ps1
```

Expected: all tests pass and the allocation sentinel reports `0 B`.

- [x] **Step 6: Commit**

```powershell
git add src/MqttZero.Server/Routing/MqttZeroPersistentHashMap* tests/MqttZero.Tests/Routing/PersistentHashMapTests.cs
git commit -m "feat: add persistent route hash map"
```

---

### Task 2: Topic Segments, Route Trie and Immutable Epoch

**Files:**

- Create: `src/MqttZero.Server/Routing/MqttZeroTopicSegment.cs`
- Create: `src/MqttZero.Server/Routing/MqttZeroTopicSegmentComparer.cs`
- Create: `src/MqttZero.Server/Routing/MqttZeroSharedGroupKey.cs`
- Create: `src/MqttZero.Server/Routing/MqttZeroRouteChild.cs`
- Create: `src/MqttZero.Server/Routing/MqttZeroRouteEndpoint.cs`
- Create: `src/MqttZero.Server/Routing/MqttZeroSharedRouteEntries.cs`
- Create: `src/MqttZero.Server/Routing/MqttZeroRouteTrieNode.cs`
- Create: `src/MqttZero.Server/Routing/MqttZeroRouteEpoch.cs`
- Create: `src/MqttZero.Server/Routing/MqttZeroRouteEpochMatch.cs`
- Create: `tests/MqttZero.Tests/Routing/RouteEpochTests.cs`

**Interfaces:**

- Produces: `MqttZeroTopicSegment.From(ReadOnlySpan<byte>)`; segment storage is immutable and hashes with FNV-1a.
- Produces: `MqttZeroSharedGroupKey(ReadOnlySpan<byte> group, ReadOnlySpan<byte> filter)`.
- Produces: `MqttZeroRouteEpoch.Empty`.
- Produces: `MqttZeroRouteEpoch(long version, MqttZeroRouteTrieNode root)`.
- Produces:
  - `MqttZeroRouteEpoch Add(MqttZeroSessionState session, MqttZeroSubscription subscription)`
  - `MqttZeroRouteEpoch Remove(MqttZeroSessionState session, MqttZeroSubscription subscription)`
  - `void Match(ReadOnlySpan<byte> topic, MqttZeroSessionState? sender, ref MqttZeroRouteEpochMatch match)`
- `MqttZeroSharedRouteEntries` stores one shared-group/filter endpoint and its `MqttZeroPersistentHashMap<MqttZeroSessionState, MqttZeroSubscription>` entries.
- `MqttZeroRouteTrieNode` stores:
  - `MqttZeroRouteEndpoint? Endpoint`
  - `MqttZeroRouteTrieNode? SingleLevel`
  - `MqttZeroRouteTrieNode? MultiLevel`
  - `ReadOnlySpan<MqttZeroRouteChild> ExactChildren`
- Exact children are immutable arrays sorted by raw UTF-8 segment bytes; matching uses allocation-free binary search over the topic span.
- `MqttZeroRouteEndpoint` stores regular Session entries and shared Session entries grouped by `MqttZeroSharedGroupKey`.
- Produces: `internal MqttZeroRouteTrieNode? GetNodeForTest(ReadOnlySpan<byte> filter)` for structural-sharing tests only.

- [x] **Step 1: Port all existing topic/trie behavior into failing Epoch tests**

```csharp
[Theory]
[InlineData("foo", "foo", 1)]
[InlineData("foo", "foo/", 0)]
[InlineData("foo/+", "foo/", 1)]
[InlineData("foo/#", "foo", 1)]
[InlineData("foo/#", "foo/bar", 1)]
[InlineData("+/bar", "/bar", 1)]
public void EpochPreservesMqttWildcardSemantics(
    string filter,
    string topic,
    int expectedCount)
{
    MqttZeroSessionState session = new("receiver");
    MqttZeroSubscription subscription = new(
        Encoding.UTF8.GetBytes(filter),
        MqttQualityOfServiceLevel.AtMostOnce,
        false,
        false,
        null);
    MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty
        .Add(session, subscription);
    MqttZeroRouteEpochMatch match = new();

    epoch.Match(Encoding.UTF8.GetBytes(topic), null, ref match);

    Assert.Equal(expectedCount, match.Count);
}
```

- [x] **Step 2: Add reference-sharing update tests**

```csharp
[Fact]
public void UpdatingOneFilterSharesUnchangedTriePaths()
{
    MqttZeroSessionState a = new("a");
    MqttZeroSessionState b = new("b");
    MqttZeroSubscription alphaOne = Create("alpha/one");
    MqttZeroSubscription alphaTwo = Create("alpha/two");
    MqttZeroSubscription beta = Create("beta/#");

    MqttZeroRouteEpoch first = MqttZeroRouteEpoch.Empty
        .Add(a, alphaOne)
        .Add(a, alphaTwo)
        .Add(b, beta);
    MqttZeroRouteEpoch second = first.Add(b, Create("alpha/one"));

    Assert.Same(
        first.GetNodeForTest("beta"),
        second.GetNodeForTest("beta"));
    Assert.NotSame(
        first.GetNodeForTest("alpha/one"),
        second.GetNodeForTest("alpha/one"));
}
```

- [x] **Step 3: Run tests and verify RED**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~RouteEpochTests"
```

Expected: compilation fails because `MqttZeroRouteEpoch` does not exist.

- [x] **Step 4: Implement path-copy updates**

Split a filter into topic levels without converting it to UTF-16. Parse `$share/<group>/<filter>` before trie insertion. `Add` and `Remove` copy only the `ExactChildren` nodes on the filter path plus the terminal endpoint. Existing exact child subtrees, single-level and multi-level nodes remain reference-equal.

The match walker emits regular entries and shared endpoint groups directly into an Epoch match buffer:

```csharp
internal ref struct MqttZeroRouteEpochMatch
{
    public void AddRegular(
        MqttZeroSessionState session,
        MqttZeroSubscription subscription,
        MqttZeroSessionState? sender);

    public void AddSharedGroup(
        MqttZeroSharedGroupKey key,
        MqttZeroSharedRouteEntries entries,
        ReadOnlySpan<byte> topic,
        MqttZeroSessionState? sender,
        MqttZeroSharedGroupCursors cursors);
}
```

- [x] **Step 5: Add mixed regular/shared tests**

Cover:

- shared group targets exactly one Session per group/filter,
- `No Local` still removes the only candidate,
- a Session matching regular and shared filters is deduplicated by the later planner,
- Subscription Identifiers are collected,
- highest QoS wins for the same Session,
- `$share` group and filter bytes remain distinct,
- trailing empty levels behave exactly as the current matcher.

- [x] **Step 6: Run tests and allocation sentinel**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~RouteEpochTests|FullyQualifiedName~TopicTrieTests"
perf/verify-allocation.ps1
```

- [x] **Step 7: Commit**

```powershell
git add src/MqttZero.Server/Routing tests/MqttZero.Tests/Routing
git commit -m "feat: add immutable route epoch trie"
```

---

### Task 3: Epoch Lifecycle and Atomic Subscription Persistence

**Files:**

- Create: `src/MqttZero.Server/Routing/MqttZeroRouteEpochManager.cs`
- Create: `src/MqttZero.Server/Broker/MqttZeroBroker.RouteEpochs.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Packets.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.ClientManagement.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Persistence.cs`
- Create: `tests/MqttZero.Tests/Routing/RouteEpochManagerTests.cs`
- Modify: `tests/MqttZero.Tests/Broker/BrokerIntegrationTests.Subscriptions.cs`

**Interfaces:**

- Produces: `MqttZeroRouteEpochManager.Current`.
- Produces:
  - `MqttZeroRouteEpoch BuildAdd(IReadOnlyList<MqttZeroSubscription> subscriptions, MqttZeroSessionState session)`
  - `MqttZeroRouteEpoch BuildRemove(IReadOnlyList<MqttZeroSubscription> subscriptions, MqttZeroSessionState session)`
  - `void Publish(MqttZeroRouteEpoch expected, MqttZeroRouteEpoch replacement)`
- Produces: `SaveSessionSubscriptionsAsync(MqttZeroSessionState session, IReadOnlyList<MqttZeroSubscription> candidate, CancellationToken)`.
- Broker replaces `_subscriptionRouteSnapshot` with `MqttZeroRouteEpochManager _routeEpochs`.

- [x] **Step 1: Write failing persistence-order tests**

```csharp
[Fact]
public async Task SubscriptionIsNotVisibleWhenPersistenceFails()
{
    RecordingSessionStore store = new()
    {
        SaveException = new IOException("expected")
    };
    MqttZeroBroker broker = CreateBrokerWithStore(store);
    MqttZeroSessionState session = CreateConnectedSession(broker, "subscriber");
    MqttZeroRouteEpoch before = broker.GetRouteEpochForTest();

    await Assert.ThrowsAsync<IOException>(() =>
        broker.ApplySubscriptionsForTestAsync(
            session,
            [CreateRequest("failure/#")]));

    Assert.Empty(session.Subscriptions);
    Assert.Same(before, broker.GetRouteEpochForTest());
}
```

```csharp
[Fact]
public async Task SubscriptionBecomesVisibleAfterPersistenceSucceeds()
{
    RecordingSessionStore store = new();
    MqttZeroBroker broker = CreateBrokerWithStore(store);
    MqttZeroSessionState session = CreateConnectedSession(broker, "subscriber");

    await broker.ApplySubscriptionsForTestAsync(
        session,
        [CreateRequest("success/#")]);

    Assert.Single(session.Subscriptions);
    Assert.Equal(1, store.SaveCount);
    Assert.NotSame(MqttZeroRouteEpoch.Empty, broker.GetRouteEpochForTest());
}
```

- [x] **Step 2: Run tests and verify RED**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~RouteEpochManagerTests|FullyQualifiedName~SubscriptionIsNotVisibleWhenPersistenceFails"
```

- [x] **Step 3: Implement the transaction**

For each SUBSCRIBE/UNSUBSCRIBE batch:

1. Acquire the existing per-Session mutation lease.
2. Create a candidate subscription array without touching `session.Subscriptions`.
3. Build the next Epoch from the current Epoch and candidate delta.
4. Save the candidate subscription array while the current session fields remain available for serialization.
5. On persistence failure, discard the candidate and keep the old Epoch and Session list.
6. On success, replace `session.Subscriptions` and call `_routeEpochs.Publish(current, next)` under the mutation lease.
7. Send SUBACK/UNSUBACK only after the Epoch is published.

Add an optional subscription candidate parameter to the persistence serializer rather than mutating first and rolling back.

- [x] **Step 4: Replace every rebuild call site**

Remove `RebuildSubscriptionRouteSnapshot()` calls from:

- packet SUBSCRIBE/UNSUBSCRIBE,
- connected-client management,
- session connect/takeover,
- persistence restore,
- session deletion.

Restore builds one Epoch after all staged Sessions are installed and publishes it once.

- [x] **Step 5: Run subscription and persistence tests**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~RouteEpochManagerTests|FullyQualifiedName~BrokerIntegrationTests.Subscriptions|FullyQualifiedName~PersistenceStoreTests"
```

- [x] **Step 6: Commit**

```powershell
git add src/MqttZero.Server/Broker src/MqttZero.Server/Routing/MqttZeroRouteEpochManager.cs tests/MqttZero.Tests
git commit -m "feat: publish route epochs after persistence"
```

---

### Task 4: Stable Shared-Group Cursors

**Files:**

- Create: `src/MqttZero.Server/Routing/MqttZeroSharedGroupCursor.cs`
- Create: `src/MqttZero.Server/Routing/MqttZeroSharedGroupCursors.cs`
- Modify: `src/MqttZero.Server/Routing/MqttZeroRouteEpochMatch.cs`
- Create: `tests/MqttZero.Tests/Routing/SharedGroupCursorTests.cs`

**Interfaces:**

- Produces: `MqttZeroSharedGroupCursors.GetNext(MqttZeroSharedGroupKey key, int count)`.
- Produces: `int GetCursor(MqttZeroSharedGroupKey key)` for tests and diagnostics.
- Cursor storage is a `ConcurrentDictionary<MqttZeroSharedGroupKey, MqttZeroSharedGroupCursor>`.
- `MqttZeroSharedGroupCursor.NextOffset(int count)` uses unsigned modulo and `Interlocked.Increment`.

- [x] **Step 1: Write failing cursor-stability tests**

```csharp
[Fact]
public void CursorSurvivesEpochReplacement()
{
    MqttZeroSessionState a = new("a");
    MqttZeroSessionState b = new("b");
    MqttZeroSharedGroupCursors cursors = new();
    MqttZeroRouteEpoch first = MqttZeroRouteEpoch.Empty
        .Add(a, CreateShared("workers", "jobs/#"))
        .Add(b, CreateShared("workers", "jobs/#"));

    Assert.Equal("a", SelectSingle(first, cursors));

    MqttZeroRouteEpoch second = first.Add(
        new MqttZeroSessionState("c"),
        CreateShared("workers", "jobs/#"));
    Assert.Equal("b", SelectSingle(second, cursors));
}
```

- [x] **Step 2: Write failing key-isolation test**

Two groups and two filters must not share cursors:

```csharp
Assert.NotEqual(
    cursors.GetCursor(new MqttZeroSharedGroupKey("one"u8, "a/#"u8)),
    cursors.GetCursor(new MqttZeroSharedGroupKey("two"u8, "a/#"u8)));
```

- [x] **Step 3: Run tests and verify RED**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~SharedGroupCursorTests"
```

- [x] **Step 4: Implement cursor registry and integrate matching**

Keep cursor objects across Epoch replacement. Do not bake a mutable cursor into a Trie node. Shared selection must first count only eligible candidates after `No Local` and topic matching, then select the offset.

- [x] **Step 5: Run tests**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~SharedGroupCursorTests|FullyQualifiedName~RouteEpochTests"
```

- [x] **Step 6: Commit**

```powershell
git add src/MqttZero.Server/Routing tests/MqttZero.Tests/Routing/SharedGroupCursorTests.cs
git commit -m "feat: preserve shared route cursors"
```

---

### Task 5: Shard-Aware Route Planner

**Files:**

- Create: `src/MqttZero.Server/Routing/MqttZeroPlannedDelivery.cs`
- Create: `src/MqttZero.Server/Routing/MqttZeroFanoutBatch.cs`
- Create: `src/MqttZero.Server/Routing/MqttZeroRoutePlan.cs`
- Create: `src/MqttZero.Server/Routing/MqttZeroRoutePlanBuffer.cs`
- Create: `src/MqttZero.Server/Routing/MqttZeroRoutePlanner.cs`
- Create: `src/MqttZero.Server/Routing/MqttZeroRoutePlannerLanes.cs`
- Create: `tests/MqttZero.Tests/Routing/RoutePlannerTests.cs`

**Interfaces:**

- `MqttZeroPlannedDelivery` stores Session, delivery QoS, Retain As Published and a reference to reusable Subscription Identifier storage.
- `MqttZeroFanoutBatch` exposes:
  - `int ShardId`
  - `int Count`
  - `MqttZeroPlannedDelivery this[int index]`
- `MqttZeroRoutePlan` owns an array of per-Shard `MqttZeroFanoutBatch` values.
- `MqttZeroRoutePlan` exposes `ReadOnlySpan<MqttZeroFanoutBatch> Batches`, `int NonEmptyBatchCount` and `int TotalDeliveryCount`.
- `MqttZeroRoutePlanner.Plan(MqttZeroRouteEpoch epoch, ReadOnlySpan<byte> topic, MqttZeroSessionState? sender, MqttZeroSharedGroupCursors cursors, int shardCount, MqttZeroRoutePlanBuffer buffer)`.
- `MqttZeroRoutePlannerLanes` owns one `MqttZeroRoutePlanBuffer` per publisher Shard and exposes `this[int shardId]`.

- [x] **Step 1: Write failing deduplication and merge tests**

```csharp
[Fact]
public void OneSessionMatchingThreeFiltersProducesOneDelivery()
{
    MqttZeroSessionState session = new("subscriber", 100, shardId: 3);
    MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty
        .Add(session, Create("route/#", MqttQualityOfServiceLevel.AtMostOnce, 1))
        .Add(session, Create("route/+", MqttQualityOfServiceLevel.AtLeastOnce, 2))
        .Add(session, Create("route/topic", MqttQualityOfServiceLevel.ExactlyOnce, 3));
    MqttZeroRoutePlanBuffer buffer = new(8);

    MqttZeroRoutePlan plan = MqttZeroRoutePlanner.Plan(
        epoch,
        "route/topic"u8,
        null,
        new MqttZeroSharedGroupCursors(),
        8,
        buffer);

    MqttZeroPlannedDelivery delivery =
        Assert.Single(plan.Batches[3].AsSpan().ToArray());
    Assert.Equal(MqttQualityOfServiceLevel.ExactlyOnce, delivery.QualityOfServiceLevel);
    Assert.Equal([1U, 2U, 3U], delivery.SubscriptionIdentifiers.ToArray());
}
```

- [x] **Step 2: Write failing per-Shard grouping test**

```csharp
[Fact]
public void OneThousandSessionsProduceAtMostOneBatchPerShard()
{
    MqttZeroRoutePlanBuffer buffer = new(16);
    MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty;
    for (int index = 0; index < 1_000; index++)
    {
        epoch = epoch.Add(
            new MqttZeroSessionState($"s-{index}", 100, index % 8),
            Create("fanout/#", MqttQualityOfServiceLevel.AtMostOnce, null));
    }

    MqttZeroRoutePlan plan = MqttZeroRoutePlanner.Plan(
        epoch,
        "fanout/topic"u8,
        null,
        new MqttZeroSharedGroupCursors(),
        8,
        buffer);

    Assert.Equal(8, plan.NonEmptyBatchCount);
    Assert.Equal(1_000, plan.TotalDeliveryCount);
}
```

- [x] **Step 3: Run tests and verify RED**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~RoutePlannerTests"
```

- [x] **Step 4: Implement the planner**

Use open-addressed per-Shard Session tables with generation stamps. Do not allocate dictionaries or lists inside `Plan`.

For each Session:

1. If `No Local` and sender matches, skip.
2. Choose the subscription with highest QoS and its Retain As Published flag.
3. Merge all matching Subscription Identifiers in stable filter traversal order.
4. Store the delivery in `batch[delivery.Session.ShardId]`.

Shared entries are planned after regular entries. The planner removes duplicate or no-local candidates before cursor selection, selects one candidate per shared group, then merges it with an existing regular delivery for the same Session.

- [x] **Step 5: Add zero-allocation tests**

```csharp
[Fact]
public void PlanningDoesNotAllocateAfterWarmup()
{
    MqttZeroRouteEpoch testEpoch = CreatePlanEpoch(100);
    MqttZeroRoutePlannerLanes lanes = new(8);
    MqttZeroRoutePlanBuffer buffer = lanes[0];
    for (int index = 0; index < 10; index++)
    {
        _ = MqttZeroRoutePlanner.Plan(
            testEpoch,
            "route/topic"u8,
            null,
            new MqttZeroSharedGroupCursors(),
            8,
            buffer);
    }

    long before = GC.GetAllocatedBytesForCurrentThread();
    for (int index = 0; index < 1_000; index++)
    {
        _ = MqttZeroRoutePlanner.Plan(
            testEpoch,
            "route/topic"u8,
            null,
            new MqttZeroSharedGroupCursors(),
            8,
            buffer);
    }

    Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);
}
```

`CreatePlanEpoch(int count)` adds `count` Sessions with `ShardId = index % 8` and a `route/#` subscription.

- [x] **Step 6: Run planner and allocation tests**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~RoutePlannerTests"
perf/verify-allocation.ps1
```

- [x] **Step 7: Commit**

```powershell
git add src/MqttZero.Server/Routing/MqttZero*Plan* src/MqttZero.Server/Routing/MqttZeroPlannedDelivery.cs src/MqttZero.Server/Routing/MqttZeroFanoutBatch.cs tests/MqttZero.Tests/Routing/RoutePlannerTests.cs
git commit -m "feat: plan shard aggregated fanout"
```

---

### Task 6: Sharded Fanout Dispatch

**Files:**

- Create: `src/MqttZero.Server/Broker/MqttZeroFanoutBatchWork.cs`
- Create: `src/MqttZero.Server/Broker/MqttZeroFanoutOperation.cs`
- Create: `src/MqttZero.Server/Broker/MqttZeroFanoutDispatcher.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.FastPath.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Delivery.cs`
- Create: `tests/MqttZero.Tests/Broker/FanoutDispatcherTests.cs`

**Interfaces:**

- `MqttZeroFanoutBatchWork : MqttZeroShardWork` is pooled and owns Operation, Shard ID and delivery range.
- `MqttZeroFanoutOperation` implements `IValueTaskSource<bool>` and owns:
  - `MqttZeroRoutePlanBuffer`
  - `MqttZeroMessageLease`
  - `MqttZeroClientConnection? senderConnection`
  - remaining batch count
  - failure state
- Produces: `ValueTask DispatchAsync(MqttZeroRoutePlan plan, MqttZeroRoutePlanBuffer buffer, MqttZeroMessageLease lease, MqttZeroClientConnection? senderConnection, int publisherShardId, CancellationToken cancellationToken)`.
- Planned Subscription Identifiers are copied into message-owned pooled storage before the planner lane can be reused.

- [x] **Step 1: Write failing Work-Item-count test**

Use a recording `MqttZeroShardWork` hook or Event Loop diagnostics. Record enqueue count before and after one 1,000-Session publish distributed across eight Shards.

```csharp
Assert.InRange(remoteEnqueueDelta, 0, 7);
Assert.Equal(1_000, receiver.PendingMessages.Count across all sessions);
```

- [x] **Step 2: Write failing ordering test**

Publish payloads `0`, `1`, `2` from one publisher to Sessions on multiple Shards. Wait for all target batches. Assert each Session receives `[0,1,2]`.

- [x] **Step 3: Write failing lease-lifetime test**

Use a tracking `MqttZeroMessageLease`. Assert its reference count returns to zero only after every target batch completes or faults.

- [x] **Step 4: Run tests and verify RED**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~FanoutDispatcherTests"
```

- [x] **Step 5: Implement dispatch**

Dispatch algorithm:

1. Build the plan in the publisher Shard lane buffer.
2. Retain the lease once for the operation.
3. Execute the publisher Shard batch inline.
4. Rent one pooled `MqttZeroFanoutBatchWork` for each non-empty non-current Shard.
5. Enqueue every remote work item before awaiting the operation.
6. Await `MqttZeroFanoutOperation` completion.
7. Complete the publisher PUBACK only after all target Shard batches have enqueued into Session Pending queues, not after subscriber Flush.
8. Release the operation buffer and lease.

Each remote batch loops its `MqttZeroPlannedDelivery` values in array order and calls the existing delivery enqueue path with the planned QoS, retain flag and identifiers. Signal each touched Egress Actor after its Session message is enqueued.

- [x] **Step 6: Run fanout and QoS tests**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~FanoutDispatcherTests|FullyQualifiedName~OutboundDeliveryTests|FullyQualifiedName~BrokerIntegrationTests.Qos"
```

- [x] **Step 7: Commit**

```powershell
git add src/MqttZero.Server/Broker src/MqttZero.Server/Routing tests/MqttZero.Tests/Broker/FanoutDispatcherTests.cs
git commit -m "feat: dispatch fanout by target shard"
```

---

### Task 7: Retained Topic Trie

**Files:**

- Create: `src/MqttZero.Server/Retained/MqttZeroRetainedTopicTrie.cs`
- Create: `src/MqttZero.Server/Retained/MqttZeroRetainedTopicNode.cs`
- Create: `src/MqttZero.Server/Retained/MqttZeroRetainedTopicChild.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Retained.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Packets.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.ClientManagement.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Persistence.cs`
- Create: `tests/MqttZero.Tests/Retained/RetainedTopicTrieTests.cs`

**Interfaces:**

- Produces: `MqttZeroRetainedTopicTrie.Empty`.
- Produces:
  - `MqttZeroRetainedTopicTrie Set(ReadOnlySpan<byte> topic, MqttZeroRetainedMessage message)`
  - `MqttZeroRetainedTopicTrie Remove(ReadOnlySpan<byte> topic)`
  - `void Match(ReadOnlySpan<byte> filter, List<MqttZeroRetainedMessage> results)`
- Trie nodes use immutable exact-topic child maps and an optional retained leaf.

- [x] **Step 1: Write failing retained wildcard tests**

```csharp
[Fact]
public void MatchesRetainedTopicsWithoutEnumeratingTheDictionary()
{
    MqttZeroRetainedTopicTrie trie = MqttZeroRetainedTopicTrie.Empty
        .Set("devices/a/state"u8, Message("devices/a/state"))
        .Set("devices/b/state"u8, Message("devices/b/state"))
        .Set("other/a/state"u8, Message("other/a/state"));
    List<MqttZeroRetainedMessage> results = [];

    trie.Match("devices/+/state"u8, results);

    Assert.Equal(
        ["devices/a/state", "devices/b/state"],
        results.Select(message => Encoding.UTF8.GetString(message.Topic.Span)));
}
```

- [x] **Step 2: Write failing update/persistence transaction tests**

Assert:

- retained replacement returns the old lease after the new trie is published,
- zero-byte retained publish removes the topic,
- failed retained Store save leaves both dictionary and trie unchanged,
- restore publishes the complete retained trie once,
- expiry removal updates both dictionary and trie.

- [x] **Step 3: Run tests and verify RED**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~RetainedTopicTrieTests"
```

- [x] **Step 4: Replace retained scans**

Replace both retained delivery scans with `Match(filter, scratchResults)`. Keep `_retainedMessages` for administration and persistence compatibility until Phase 8, but routing reads only the immutable trie and a volatile reference.
Retain each matched message while reading the immutable snapshot so concurrent replacement cannot recycle its Lease before delivery finishes.

- [x] **Step 5: Run retained tests**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~RetainedTopicTrieTests|FullyQualifiedName~BrokerIntegrationTests.Retained|FullyQualifiedName~RetainedLeaseTests"
```

- [x] **Step 6: Commit**

```powershell
git add src/MqttZero.Server/Retained src/MqttZero.Server/Broker tests/MqttZero.Tests/Retained
git commit -m "feat: index retained messages by topic"
```

---

### Task 8: Remove Legacy Routing and Record Phase 4 Baseline

**Files:**

- Delete: `src/MqttZero.Server/Routing/MqttZeroSubscriptionRouteSnapshot.cs`
- Delete: `src/MqttZero.Server/Routing/MqttZeroSubscriptionRouteSnapshotBuilder.cs`
- Delete: `src/MqttZero.Server/Routing/MqttZeroRouteFilterGroup.cs`
- Delete: `src/MqttZero.Server/Routing/MqttZeroRouteScope.cs`
- Delete: `src/MqttZero.Server/Routing/MqttZeroRouteScopePool.cs`
- Delete: `src/MqttZero.Server/Routing/MqttZeroTopicTrie.cs`
- Delete: `tests/MqttZero.Tests/Routing/RouteAllocationTests.cs`
- Delete: `tests/MqttZero.Tests/Routing/TopicTrieTests.cs`
- Delete: `benchmarks/MqttZero.Benchmarks/RoutingBenchmarks.cs`
- Delete: `benchmarks/MqttZero.Benchmarks/SharedRoutingBenchmarks.cs`
- Delete: `benchmarks/MqttZero.Benchmarks/FanoutBenchmarks.cs`
- Create: `benchmarks/MqttZero.Benchmarks/RouteEpochBenchmarks.cs`
- Create: `benchmarks/MqttZero.Benchmarks/RoutePlanningBenchmarks.cs`
- Create: `perf/baseline/broker-phase4-2026-11-25.json`
- Create: `perf/baseline/broker-phase4-2026-11-25.md`
- Modify: `perf/baseline/README.md`
- Modify: `README.md`

- [x] **Step 1: Add benchmarks**

`RouteEpochBenchmarks` parameters:

- subscribers: `1`, `100`, `1000`
- operation: exact, `+`, `#`

`RoutePlanningBenchmarks` parameters:

- subscribers: `100`, `1000`
- Shards: `1`, `8`
- regular and shared groups

- [x] **Step 2: Run the development matrix**

```powershell
dotnet run -c Release --no-build `
    --project benchmarks/MqttZero.BrokerBenchmarks -- `
    compare --client mqttnet --qos 0,1 --subscribers 1,100 `
    --payload 32,8192,65536 --warmup 2 --seconds 6 `
    --repetitions 3 --max-pending 1024 --start-port 36000 `
    --output artifacts/broker-phase4-dev.json
```

- [x] **Step 3: Compare against Phase 0**

```powershell
./eng/compare-broker-benchmarks.ps1 `
    -Baseline perf/baseline/broker-phase0-2026-11-25.json `
    -Current artifacts/broker-phase4-dev.json `
    -MaxThroughputRegression 0.05 `
    -MaxP99Regression 0.05
```

Expected: all 12 cells pass. If any cell fails, keep the red result and iterate on planner buffer layout or batch dispatch; do not change benchmark parameters.

- [x] **Step 4: Run the official baseline**

```powershell
./eng/run-broker-benchmark.ps1 `
    -Client mqttnet `
    -OutputPath perf/baseline/broker-phase4-2026-11-25.json `
    -StartPort 30000
```

The command may return non-zero while the strict MQTTnet comparison is still red. The JSON must remain complete.

- [x] **Step 5: Run full verification**

```powershell
dotnet restore MqttZero.slnx --locked-mode
dotnet build MqttZero.slnx -c Release --no-restore
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-build
dotnet test tests/MqttZero.Dashboard.Tests/MqttZero.Dashboard.Tests.csproj -c Release --no-build
dotnet test tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release --no-build
./eng/verify-coverage.ps1
./perf/verify-allocation.ps1
dotnet format MqttZero.slnx --verify-no-changes --no-restore
git diff --check
```

- [x] **Step 6: Write the Phase 4 report**

Include:

- Epoch update count and maximum copied node depth;
- shared cursor retention evidence;
- per-publish non-current Shard Work Item count;
- zero-allocation planning evidence;
- Phase 0/4 cell-by-cell comparisons;
- the QoS 0/100/8 KiB result that motivated Phase 4;
- remaining strict MQTTnet failures without overstating completion.

- [x] **Step 7: Commit**

```powershell
git add benchmarks src tests perf/baseline README.md docs/superpowers/plans/2026-11-25-phase-4-route-epoch-fanout.md
git commit -m "perf: implement route epoch fanout"
```

---

## Test Plan Summary

- HAMT: insert, update, remove, collisions, enumeration, structural sharing, zero allocation.
- Trie: exact, `+`, `#`, trailing empty levels, Unicode byte filters, shared filter parsing.
- Epoch: immutable reference sharing, old readers remain valid, no in-place mutation.
- Subscription transaction: persistence failure leaves Session and Epoch unchanged.
- Shared group: one eligible Session per group, stable cursor across Epochs, `No Local`, identifier merge.
- Planner: one Session delivery across multiple filters, highest QoS, stable identifier order, one batch per Shard.
- Dispatcher: at most one remote Work Item per Shard, three-publish ordering, lease lifetime, failure isolation.
- Retained: wildcard matching, replacement/removal, expiry, Store failure rollback.
- Regression: all Broker, Dashboard, Benchmark tests, coverage, allocation and formatting.
- Performance: Phase 0 development tolerance for every cell; strict MQTTnet comparison recorded but not falsely declared passed.

## Assumptions

- Phase 2 remains the production Egress implementation; Phase 4 changes route planning and ownership only.
- Session Shard ID is immutable for the Broker lifetime and is already available on `MqttZeroSessionState`.
- One publisher Shard executes at most one route-planning operation at a time because inbound connection processing is serialized on that Shard.
- The current publisher Shard executes its local batch inline and waits only for non-current Shard batches to complete enqueue.
- Persistence may be slow; Phase 4 preserves current ordering and moves persistence isolation to Phase 6.
- Shared cursors remain in memory for the Broker lifetime; bounded cleanup can be added when shared groups are removed.
- The final 27-cell MQTTnet strict gate remains in Phase 8 even if Phase 4 passes the development regression gate.
