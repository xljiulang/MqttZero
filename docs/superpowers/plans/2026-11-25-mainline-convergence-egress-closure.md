# MQTTZero 主线收束与 Egress 闭环 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 停止在 94 提交性能分支继续堆叠微优化，先把已验证收益收束到 `master` 并发布 `0.11.0-alpha.2`，再在独立分支只做一次完整的连接 Egress Actor 闭环实验。

**Architecture:** 普通 alpha 发布以正确性、零分配、稳定性和相对基线 5% 非回退为门禁；严格 MQTTnet 矩阵降为性能声明门禁。Egress 实验把 Session Pending、Inflight、编码、PipeWriter 写入和 Flush 生命周期统一到一个连接级状态机；未达到目标场景门限即删除实验。

**Tech Stack:** .NET 10, Kestrel PipeWriter, MQTT 5, Event Loop Shards, MQTTnet 5.2.0.1603 comparison client, xUnit v3, BenchmarkDotNet, PowerShell 7.

**Spec:** `docs/sharded-actor-broker-plan.md` Phase 2, `perf/baseline/broker-phase4.6-architecture-review-2026-11-25.md`, and the current `master..perf/outperform-mqttnet` history.

## Global Constraints

- Preserve public APIs, `MqttZeroOptions` defaults, MQTT wire semantics, Kestrel as the production transport, interceptors, observers, stores, Dashboard and management APIs.
- Ordinary alpha releases require correctness, stability, allocation sentinels and no throughput regression above 5%; they do not require every strict MQTTnet cell to pass.
- Strict MQTTnet comparison remains in the nightly performance workflow and `perf/baseline` as a performance-claim gate only.
- The complete Egress experiment is allowed to break internal Session, Pending, Inflight, Connection and Egress contracts.
- The Egress experiment gets one complete attempt. If the primary target or any critical regression gate fails, delete the production experiment and stop the broad "outperform MQTTnet" release goal.
- Kestrel remains the only production transport; do not revive the rejected Socket prototype or direct large `WriteAsync` experiment.
- Steady-state synchronized hot paths must remain `0 B/op` after warmup; asynchronous scheduling and exception paths are reported separately.
- No per-connection permanent Task, unbounded queue, fire-and-forget unobserved Task or alternate write owner.
- Keep one active writer per connection, preserve control-packet priority and complete MQTT packet boundaries.
- C# files use CRLF; one top-level type per file; all public APIs have XML comments; all if/method bodies use braces; `csharp-code-rules` applies.
- All commands use PowerShell 7 in the current worktree.

---

## Stage 1: Mainline Convergence and `0.11.0-alpha.2`

### Task 1: Classify the Performance History

**Files:**
- Create: `docs/performance-branch-convergence.md`
- Read: `git log master..perf/outperform-mqttnet`
- Read: `perf/baseline/*.md`
- Read: `docs/sharded-actor-broker-plan.md`

**Interfaces:**
- Produces: a reviewed commit inventory with one of `keep`, `neutral`, or `reject` for every commit or coherent commit group.
- Produces: a list of tests and baselines that justify every retained high-risk change.

- [ ] **Step 1: Inventory all commits**

  Run:

  ```powershell
  git log --reverse --format="%h %s" master..perf/outperform-mqttnet
  git diff --stat master..perf/outperform-mqttnet
  ```

  Write every commit or coherent commit group into `docs/performance-branch-convergence.md`.

- [ ] **Step 2: Classify each item**

  Use these categories:

  ```text
  keep: correctness, zero-allocation, ownership, queue, routing, memory density, protocol, benchmark infrastructure
  neutral: stable internal abstraction required by a kept item
  reject: rejected Socket transport, direct large WriteAsync, failed scheduling or batch experiments, superseded experimental paths
  ```

  Reject a commit when its only claimed benefit is a red benchmark cell or an experiment explicitly rejected in a baseline document.

- [ ] **Step 3: Verify the inventory**

  Run:

  ```powershell
  rg -n "keep|neutral|reject" docs/performance-branch-convergence.md
  ```

  Expected:

  ```text
  Every commit from master..perf/outperform-mqttnet is represented once.
  Every rejected production path has a baseline or test reason.
  ```

- [ ] **Step 4: Commit**

  ```powershell
  git add docs/performance-branch-convergence.md
  git commit -m "docs: classify performance branch history"
  ```

### Task 2: Create the Release Branch

**Files:**
- Git branches and tags only
- Read: `docs/performance-branch-convergence.md`

**Interfaces:**
- Produces: `research/sharded-egress-phase4.6` preserving the complete experiment history.
- Produces: `release/0.11-alpha.2` based on `master`.

- [ ] **Step 1: Preserve the research history**

  Run:

  ```powershell
  git branch research/sharded-egress-phase4.6 perf/outperform-mqttnet
  git push origin research/sharded-egress-phase4.6
  ```

- [ ] **Step 2: Create the release branch**

  Run:

  ```powershell
  git switch master
  git switch -c release/0.11-alpha.2
  ```

- [ ] **Step 3: Confirm the base**

  Run:

  ```powershell
  git merge-base --is-ancestor master HEAD
  git status --short --branch
  ```

  Expected: the command exits successfully and the worktree is clean.

### Task 3: Import Only Verified Correctness and Ownership Changes

**Files:**
- Modify: `src/MqttZero/Memory/MqttZeroMessageLease.cs`
- Modify: `src/MqttZero/Memory/MqttZeroBufferPool.cs`
- Modify: `src/MqttZero/Protocol/*.cs`
- Modify: `src/MqttZero.Server/Sessions/*.cs`
- Modify: `src/MqttZero.Server/Routing/*.cs`
- Test: `tests/MqttZero.Tests/Memory/*.cs`
- Test: `tests/MqttZero.Tests/Protocol/*.cs`
- Test: `tests/MqttZero.Tests/Routing/*.cs`
- Test: `tests/MqttZero.Tests/Sessions/*.cs`

**Interfaces:**
- Consumes: the `keep` and required `neutral` classification from Task 1.
- Produces: correctness and ownership changes that pass independently before scheduling changes are imported.

- [ ] **Step 1: Write a focused ownership regression test**

  Add a test that creates an owned lease, retains it for an inbound work item, releases the producer reference, executes the work, and asserts the buffer-pool active-byte count returns to zero.

  Run:

  ```powershell
  dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore --filter "FullyQualifiedName~Lease"
  ```

  Expected before importing the fix: the test reproduces a missing independent reference or non-zero pool budget.

- [ ] **Step 2: Import the verified ownership implementation**

  Apply only the owned-lease, inbound-work, queue-version, buffer-pool and protocol-boundary changes classified `keep`.

- [ ] **Step 3: Run focused tests**

  Run:

  ```powershell
  dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
    --filter "FullyQualifiedName~Memory|FullyQualifiedName~Protocol|FullyQualifiedName~Sessions|FullyQualifiedName~Routing"
  ```

  Expected: all selected tests pass, including zero-allocation tests.

- [ ] **Step 4: Commit**

  ```powershell
  git add src/MqttZero src/MqttZero.Server/Sessions src/MqttZero.Server/Routing tests/MqttZero.Tests
  git commit -m "fix: converge verified lease and protocol ownership"
  ```

### Task 4: Import Verified Routing, Memory-Density and Queue Improvements

**Files:**
- Modify: `src/MqttZero.Server/Routing/*.cs`
- Modify: `src/MqttZero.Server/Retained/*.cs`
- Modify: `src/MqttZero.Server/Sessions/MqttZeroMessageQueue.cs`
- Modify: `src/MqttZero.Server/Clients/MqttZeroTopicAliasTable.cs`
- Test: `tests/MqttZero.Tests/Routing/*.cs`
- Test: `tests/MqttZero.Tests/Retained/*.cs`
- Test: `tests/MqttZero.Tests/Sessions/*.cs`
- Test: `tests/MqttZero.Tests/Broker/TopicAliasAllocationTests.cs`

**Interfaces:**
- Consumes: route epochs, persistent HAMT, shared cursors, intrusive queue and lazy alias implementations from the research branch.
- Produces: the retained routing and memory improvements without importing Egress scheduling experiments.

- [ ] **Step 1: Run the existing routing and queue tests on the release branch**

  Run:

  ```powershell
  dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
    --filter "FullyQualifiedName~Routing|FullyQualifiedName~Retained|FullyQualifiedName~MessageQueue"
  ```

  Expected: capture the baseline pass/fail set before migration.

- [ ] **Step 2: Import the retained implementations and tests**

  Apply the Route Epoch, HAMT, shared cursor, retained trie, intrusive queue, lazy Topic Alias and single-Slab lease changes classified `keep`.

- [ ] **Step 3: Run focused and allocation tests**

  Run:

  ```powershell
  dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
    --filter "FullyQualifiedName~Routing|FullyQualifiedName~Retained|FullyQualifiedName~MessageQueue|FullyQualifiedName~Allocation"
  ```

  Expected: all selected tests pass and allocation assertions report `0 B/op` where specified.

- [ ] **Step 4: Commit**

  ```powershell
  git add src/MqttZero.Server src/MqttZero tests/MqttZero.Tests
  git commit -m "perf: converge verified routing and memory density"
  ```

### Task 5: Import Benchmark Infrastructure and Correctness Gates

**Files:**
- Modify: `.github/workflows/performance.yml`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/**`
- Modify: `tests/MqttZero.BrokerBenchmarks.Tests/**`
- Modify: `eng/verify-benchmark-smoke.ps1`
- Modify: `perf/baseline/**`

**Interfaces:**
- Produces: reproducible separated-process Broker comparison, raw diagnostic client, environment capture and nightly strict performance reporting.
- Produces: no exception to the zero-failure, warmup-drain and three-repetition methodology.

- [ ] **Step 1: Import the benchmark tooling**

  Apply the benchmark configuration, topology, environment capture, raw-client and gate changes classified `keep`.

- [ ] **Step 2: Run benchmark unit tests**

  Run:

  ```powershell
  dotnet test tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release --no-restore
  ```

  Expected: all tests pass.

- [ ] **Step 3: Run smoke**

  Run:

  ```powershell
  ./eng/verify-benchmark-smoke.ps1
  ```

  Expected: both Brokers start independently, both receive traffic, both report zero failed operations and complete warmup drain.

- [ ] **Step 4: Commit**

  ```powershell
  git add .github/workflows/performance.yml benchmarks tests/MqttZero.BrokerBenchmarks.Tests eng perf
  git commit -m "perf: converge broker benchmark infrastructure"
  ```

### Task 6: Define the Ordinary Release Gate

**Files:**
- Modify: `README.md`
- Modify: `CONTRIBUTING.md`
- Modify: `perf/baseline/broker-comparison-methodology.md`
- Modify: `perf/baseline/README.md`
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/performance.yml`

**Interfaces:**
- Produces: a normal alpha gate independent of strict MQTTnet superiority.
- Produces: a separate nightly performance-claim gate.

- [ ] **Step 1: Document the two gates**

  Write the following exact policy into the documentation:

  ```text
  Normal alpha gate:
  - Release build, core tests, Dashboard tests, Vue tests, real transport tests and allocation sentinels pass.
  - No matrix cell may regress throughput by more than 5% from the same-machine stable baseline.
  - No failed operations, connection failures, drain timeouts or lease leaks are permitted.

  Performance-claim gate:
  - Three repetitions and median throughput/p99 are required.
  - A cell is "ahead of MQTTnet" only when throughput is greater than MQTTnet and p99 is not worse.
  - Cells outside the claim are reported as known gaps; they do not block an ordinary alpha build.
  ```

- [ ] **Step 2: Update CI**

  Make pull-request CI run the normal gate. Keep nightly performance running the strict matrix and uploading JSON regardless of gate result.

- [ ] **Step 3: Validate workflow syntax and documentation references**

  Run:

  ```powershell
  rg -n "Normal alpha gate|Performance-claim gate|5%|strict MQTTnet" README.md CONTRIBUTING.md perf/baseline .github/workflows
  ```

  Expected: all four policy terms are present in the release documentation and CI comments.

- [ ] **Step 4: Commit**

  ```powershell
  git add README.md CONTRIBUTING.md perf/baseline .github/workflows
  git commit -m "docs: separate alpha release and performance claim gates"
  ```

### Task 7: Update the Architecture Record

**Files:**
- Modify: `docs/sharded-actor-broker-plan.md`
- Create: `docs/performance-positioning.md`

**Interfaces:**
- Produces: an honest status for Phase 2 and a documented product positioning statement.

- [ ] **Step 1: Mark the incomplete architecture**

  In `docs/sharded-actor-broker-plan.md`, record:

  ```text
  Phase 2 is partially implemented.
  The current Egress Actor does not own the complete Session-to-PipeWriter send loop.
  The performance branch remains research evidence and does not block ordinary alpha releases.
  ```

- [ ] **Step 2: Document the product positioning**

  In `docs/performance-positioning.md`, state the strengths that are supported by evidence:

  ```text
  Protocol completeness
  Zero allocation in selected synchronized hot paths
  Low-allocation routing and payload ownership
  Controllable broker memory
  Kestrel and ASP.NET Core integration
  ```

  Also list known capacity gaps, including large-payload high-fanout cells, without claiming universal MQTTnet superiority.

- [ ] **Step 3: Commit**

  ```powershell
  git add docs/sharded-actor-broker-plan.md docs/performance-positioning.md
  git commit -m "docs: record partial egress architecture and performance position"
  ```

### Task 8: Prepare and Release `0.11.0-alpha.2`

**Files:**
- Modify: `Directory.Build.props`
- Modify: `dashboard/MqttZero.Dashboard/package.json`
- Modify: `dashboard/MqttZero.Dashboard/package-lock.json`
- Modify: `eng/verify-packages.ps1`
- Modify: `README.md`

**Interfaces:**
- Produces: package version `0.11.0-alpha.2` in source, Dashboard metadata, pack verification and consumer project.

- [ ] **Step 1: Update the version**

  Change `0.11.0-alpha.1` to `0.11.0-alpha.2` in all four version locations.

- [ ] **Step 2: Restore, build and test**

  Run:

  ```powershell
  dotnet restore MqttZero.slnx --locked-mode
  dotnet build MqttZero.slnx -c Release --no-restore
  dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-build
  dotnet test tests/MqttZero.Dashboard.Tests/MqttZero.Dashboard.Tests.csproj -c Release --no-build
  ./perf/verify-allocation.ps1
  dotnet format MqttZero.slnx --verify-no-changes --no-restore
  ```

  Expected: all commands exit with code zero and Release build has zero warnings.

- [ ] **Step 3: Verify Vue and package consumers**

  Run:

  ```powershell
  Set-Location dashboard/MqttZero.Dashboard
  npm ci
  npm test
  npm run build
  Set-Location ../..
  ./eng/verify-packages.ps1
  ```

  Expected: Vue tests and production build pass; all four NuGet packages pack; both fresh consumers restore and build outside the repository.

- [ ] **Step 4: Commit and merge**

  ```powershell
  git add Directory.Build.props dashboard/MqttZero.Dashboard/package.json dashboard/MqttZero.Dashboard/package-lock.json eng/verify-packages.ps1 README.md
  git commit -m "release: prepare MQTTZero 0.11.0-alpha.2"
  git switch master
  git merge --no-ff release/0.11-alpha.2
  git push origin master
  git push origin release/0.11-alpha.2
  ```

---

## Stage 2: Egress Closure Experiment

### Task 9: Create the Experiment Branch and Capture Baseline

**Files:**
- Create: `perf/baseline/egress-closure-baseline-2026-11-25.json`
- Create: `perf/baseline/egress-closure-baseline-2026-11-25.md`

**Interfaces:**
- Produces: tested baseline for the current `master` before any Egress architecture change.

- [ ] **Step 1: Create the branch**

  Run:

  ```powershell
  git switch master
  git switch -c perf/egress-actor-closure
  ```

- [ ] **Step 2: Run the target matrix**

  Run:

  ```powershell
  dotnet run -c Release --project benchmarks/MqttZero.BrokerBenchmarks -- `
    compare --client mqttnet --qos 0,1 `
    --subscribers 1,100 --payload 32,8192,65536 `
    --warmup 3 --seconds 10 --repetitions 3 --max-pending 1024 `
    --output perf/baseline/egress-closure-baseline-2026-11-25.json
  ```

  Expected: the JSON is complete and records every cell even if the strict gate is red.

- [ ] **Step 3: Record the target-cell baseline**

  Write the generated JSON path, machine, command and median values for `QoS1 / 100 / 64 KiB` into the Markdown baseline. This is the no-regression reference for the experiment.

- [ ] **Step 4: Commit**

  ```powershell
  git add perf/baseline/egress-closure-baseline-2026-11-25.json perf/baseline/egress-closure-baseline-2026-11-25.md
  git commit -m "docs: record egress closure baseline"
  ```

### Task 10: Add Send-Stage Attribution

**Files:**
- Modify: `src/MqttZero.Server/Broker/MqttZeroFanoutDiagnostics.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroFanoutDiagnosticsSnapshot.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkFanoutDiagnostics.cs`
- Test: `tests/MqttZero.Tests/Broker/FanoutDiagnosticsTests.cs`

**Interfaces:**
- Produces: counters and elapsed time for session-lock wait, pending selection, packet-id/inflight update, encode, PipeWriter write, flush wait, ACK processing and control-frame wait.
- Preserves: existing diagnostics names and behavior for backward-compatible benchmark JSON.

- [ ] **Step 1: Write one failing diagnostics test**

  Create a test that records a session-lock wait and an encode duration, reads the snapshot, and asserts both values are present and non-negative.

  Run:

  ```powershell
  dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
    --filter "FullyQualifiedName~FanoutDiagnosticsTests"
  ```

  Expected: fail because the new fields do not exist.

- [ ] **Step 2: Add internal counters**

  Add cumulative and maximum values using the existing `Interlocked` diagnostics style. Disabled diagnostics must take the existing branch and introduce no allocation.

- [ ] **Step 3: Expose data in outbound JSON**

  Extend `BenchmarkFanoutDiagnostics` and its JSON source-generated context so the outbound result contains the new values.

- [ ] **Step 4: Run tests and commit**

  ```powershell
  dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
    --filter "FullyQualifiedName~FanoutDiagnosticsTests"
  git add src/MqttZero.Server/Broker benchmarks/MqttZero.BrokerBenchmarks tests/MqttZero.Tests/Broker/FanoutDiagnosticsTests.cs
  git commit -m "perf: attribute egress send stages"
  ```

### Task 11: Define the Connection Egress Ownership Contract

**Files:**
- Create: `src/MqttZero.Server/Egress/MqttZeroConnectionEgress.cs`
- Create: `src/MqttZero.Server/Egress/MqttZeroConnectionEgressState.cs`
- Modify: `src/MqttZero.Server/Sessions/MqttZeroSessionState.cs`
- Modify: `src/MqttZero.Server/Clients/MqttZeroClientConnection.cs`

**Interfaces:**
- Consumes: `MqttZeroMessageQueue`, `MqttZeroQueuedMessage`, `MqttZeroClientConnection` and the Session `SyncRoot`.
- Produces: one per-connection object that exclusively owns data/control dispatch and the Flush lifecycle.
- Produces: no public API change.

- [ ] **Step 1: Write ownership tests**

  Create tests asserting:

  ```text
  Only one egress is attached to one connection.
  A second attach fails without closing the first.
  Detach removes only the matching connection/egress pair.
  Stop on one egress does not stop another connection's egress.
  ```

  Run:

  ```powershell
  dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
    --filter "FullyQualifiedName~ConnectionEgress"
  ```

  Expected: fail because the ownership type does not exist.

- [ ] **Step 2: Implement the contract**

  Add the internal egress lifetime object and attach/detach methods. Keep the existing connection writer lock until the data path migration is complete.

- [ ] **Step 3: Run ownership tests and commit**

  ```powershell
  dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
    --filter "FullyQualifiedName~ConnectionEgress"
  git add src/MqttZero.Server/Egress src/MqttZero.Server/Sessions/MqttZeroSessionState.cs src/MqttZero.Server/Clients/MqttZeroClientConnection.cs tests/MqttZero.Tests/Egress
  git commit -m "refactor: establish connection egress ownership"
  ```

### Task 12: Move Data Selection and Inflight Updates into Egress

**Files:**
- Create: `src/MqttZero.Server/Egress/MqttZeroEgressPublishBatch.cs`
- Modify: `src/MqttZero.Server/Egress/MqttZeroConnectionEgress.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Delivery.cs`
- Test: `tests/MqttZero.Tests/Broker/OutboundDeliveryTests.cs`
- Test: `tests/MqttZero.Tests/Egress/MqttZeroEgressActorTests.cs`

**Interfaces:**
- Consumes: `MqttZeroConnectionEgress` and the existing Pending/Inflight model.
- Produces: `MqttZeroEgressPublishBatch` containing only ordered `MqttZeroQueuedMessage` references.

- [ ] **Step 1: Add failing batch-selection tests**

  Cover:

  ```text
  QoS 0 ordering and release.
  QoS 1 packet-id assignment exactly once.
  Receive Maximum stops selection.
  Expiry removes stale messages before encoding.
  Lock wait is recorded separately from selection time.
  ```

  Run:

  ```powershell
  dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
    --filter "FullyQualifiedName~EgressPublishBatch"
  ```

  Expected: fail before the batch selector exists.

- [ ] **Step 2: Move selection under the Egress object**

  Move the Pending selection, expiry, packet-id assignment and Inflight insertion out of `MqttZeroBroker.Delivery.cs`. Use `session.SyncRoot` only for the shortest state mutation window.

- [ ] **Step 3: Run delivery and allocation tests**

  ```powershell
  dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
    --filter "FullyQualifiedName~OutboundDelivery|FullyQualifiedName~Egress|FullyQualifiedName~Allocation"
  ```

  Expected: all selected tests pass; warmup allocation checks remain at `0 B/op`.

- [ ] **Step 4: Commit**

  ```powershell
  git add src/MqttZero.Server/Egress src/MqttZero.Server/Broker/MqttZeroBroker.Delivery.cs tests/MqttZero.Tests
  git commit -m "refactor: move publish batch ownership into egress"
  ```

### Task 13: Unify Data, Control and Flush in One Writer

**Files:**
- Modify: `src/MqttZero.Server/Egress/MqttZeroConnectionEgress.cs`
- Modify: `src/MqttZero.Server/Egress/MqttZeroEgressActor.cs`
- Modify: `src/MqttZero.Server/Clients/MqttZeroClientConnection.cs`
- Test: `tests/MqttZero.Tests/Egress/MqttZeroEgressFlushTests.cs`
- Test: `tests/MqttZero.Tests/Egress/MqttZeroEgressSchedulerTests.cs`

**Interfaces:**
- Consumes: `MqttZeroEgressPublishBatch`, control-frame queue and connection writer methods.
- Produces: a single state machine where one execution may send multiple synchronous batches and at most one Flush is pending.
- Produces: control packets are emitted only after the current complete PUBLISH.

- [ ] **Step 1: Add deterministic scheduling tests**

  Cover:

  ```text
  Control frame waiting ends the current data batch.
  Synchronous Flush continues with the next batch.
  Asynchronous Flush suspends data until completion.
  No second Flush starts while one is pending.
  Queue exhaustion does not reschedule.
  New Signal during Flush is not lost.
  Stop and Takeover drain or cancel safely.
  ```

  Run the focused tests and confirm each newly added test fails for the expected missing ownership behavior.

- [ ] **Step 2: Route all writes through the connection Egress**

  Remove direct control/data write ownership from the connection state used by the send loop. Keep connection primitive writers as private implementation details of Egress.

- [ ] **Step 3: Add a bounded multi-batch execution loop**

  Use existing configured batch limits. Continue only after synchronous Flush completion and before the execution time budget expires. Do not add arbitrary waits or enlarge configured batch constants.

- [ ] **Step 4: Run regression and allocation tests**

  ```powershell
  dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
    --filter "FullyQualifiedName~Egress|FullyQualifiedName~OutboundDelivery|FullyQualifiedName~Qos|FullyQualifiedName~Allocation"
  ```

  Expected: all tests pass and the steady-state allocation sentinel remains green.

- [ ] **Step 5: Commit**

  ```powershell
  git add src/MqttZero.Server/Egress src/MqttZero.Server/Clients/MqttZeroClientConnection.cs tests/MqttZero.Tests
  git commit -m "refactor: unify connection egress data control and flush"
  ```

### Task 14: Remove the Old Send Paths and Add Failure Tests

**Files:**
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Delivery.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Connections.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Sessions.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Lifecycle.cs`
- Test: `tests/MqttZero.Tests/Broker/BrokerLifecycleTests.cs`
- Test: `tests/MqttZero.Tests/Broker/OutboundDeliveryTests.cs`

**Interfaces:**
- Consumes: the completed connection Egress.
- Produces: one production write path; old Drain/Wake ownership is deleted rather than retained as a fallback.

- [ ] **Step 1: Add takeover, detach and stop tests**

  Cover:

  ```text
  Takeover starts the replacement only after the old egress has stopped.
  Detach returns pending and inflight references exactly once.
  Broker stop waits for active Flush or safely cancels it.
  Flush failure is observed by readiness or connection health.
  ```

- [ ] **Step 2: Delete old send ownership**

  Remove `DrainQueuedMessagesOnceAsync` from the Broker, old per-batch connection writer ownership and duplicate scheduler wake paths that are no longer used. Retain only the Fanout Scheduler's route handoff role.

- [ ] **Step 3: Run the 1000-cycle lifecycle stress**

  ```powershell
  dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
    --filter "FullyQualifiedName~Reconnect|FullyQualifiedName~Lease|FullyQualifiedName~Takeover|FullyQualifiedName~Lifecycle"
  ```

  Expected: zero leaked active bytes, zero duplicate releases and zero unobserved Task failures.

- [ ] **Step 4: Commit**

  ```powershell
  git add src/MqttZero.Server tests/MqttZero.Tests
  git commit -m "refactor: remove superseded broker send paths"
  ```

---

## Stage 3: Measure, Decide and Release

### Task 15: Run the Egress Closure Gate

**Files:**
- Create: `perf/baseline/egress-closure-result-2026-11-25.json`
- Create: `perf/baseline/egress-closure-result-2026-11-25.md`

**Interfaces:**
- Consumes: the same machine, Release configuration and matrix as Task 9.
- Produces: a decision based on medians, not a single run.

- [ ] **Step 1: Run the primary target**

  ```powershell
  dotnet run -c Release --project benchmarks/MqttZero.BrokerBenchmarks -- `
    compare --client mqttnet --qos 1 --subscribers 100 --payload 65536 `
    --warmup 5 --seconds 20 --repetitions 3 --max-pending 1024 `
    --output perf/baseline/egress-closure-qos1-100-64k-2026-11-25.json
  ```

  Expected gate:

  ```text
  MQTTZero throughput >= baseline throughput.
  MQTTZero p99 <= baseline p99 * 0.95.
  Failed operations == 0.
  ```

- [ ] **Step 2: Run critical non-regression cells**

  ```powershell
  dotnet run -c Release --project benchmarks/MqttZero.BrokerBenchmarks -- `
    compare --client mqttnet --qos 0,1 --subscribers 1,100 `
    --payload 32,8192,65536 --warmup 3 --seconds 10 `
    --repetitions 3 --max-pending 1024 `
    --output perf/baseline/egress-closure-result-2026-11-25.json
  ```

  Acceptance:

  ```text
  Every cell throughput >= 0.95 * pre-experiment baseline.
  Every cell p99 <= 1.05 * pre-experiment baseline.
  The primary QoS1/100/64KiB cell meets its stricter target.
  ```

- [ ] **Step 3: Stop if the gate fails**

  If either criterion fails, do not tune repeatedly. Delete Tasks 11-14 production code and their experimental tests. Keep the diagnostic additions only if they are useful and zero-allocation safe. Record the failed result and stop broad MQTTnet-superiority work.

- [ ] **Step 4: Record and commit a passing result**

  If the gate passes, write machine, command, raw JSON, medians and the actual winner/loser table into `perf/baseline/egress-closure-result-2026-11-25.md`.

  ```powershell
  git add perf/baseline/egress-closure-result-2026-11-25.json perf/baseline/egress-closure-result-2026-11-25.md
  git commit -m "perf: validate connection egress closure"
  ```

### Task 16: Final Release or Experiment Removal

**Files:**
- Modify: `Directory.Build.props`
- Modify: `dashboard/MqttZero.Dashboard/package.json`
- Modify: `dashboard/MqttZero.Dashboard/package-lock.json`
- Modify: `eng/verify-packages.ps1`
- Modify: `README.md`

**Interfaces:**
- Produces: either `0.12.0-alpha.1` with the proven Egress model or a clean `0.11.0-alpha.2` mainline with the experiment removed.

- [ ] **Step 1: Execute the selected outcome**

  If Task 15 passes:

  ```text
  Version -> 0.12.0-alpha.1.
  Run full Release, core, Dashboard, Vue, allocation, format, audit, NuGet and consumer verification.
  Merge perf/egress-actor-closure to master.
  Push master.
  ```

  If Task 15 fails:

  ```text
  Delete experiment production changes.
  Retain the failed baseline report and research branch.
  Keep master at 0.11.0-alpha.2.
  Do not claim universal MQTTnet superiority.
  ```

- [ ] **Step 2: Verify final repository state**

  Run:

  ```powershell
  git status --short --branch
  git log --oneline -8
  ```

  Expected: clean worktree, intentional branch state and no uncommitted experimental code.
