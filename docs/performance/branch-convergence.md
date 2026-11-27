# Performance Branch Convergence

> 上级索引：[文档目录](../README.md)

This document classifies the history between `master` and
`perf/outperform-mqttnet` before it is migrated to the release branch.

The classification is by coherent change group. Many commits share files and
tests, so a commit-by-commit cherry-pick would duplicate partially completed
interfaces. A retained group is migrated as a tested unit.

## Keep

### Correctness and lease ownership

- `a7a6d39`, `89d3e7d`: inbound work lease ownership, queue ownership, property
  length and route boundary fixes.
- `c2f73a2`, `e340a9e`, `ae44180`: inbound dispatch lifecycle, persistence
  ordering and allocation-free acknowledgement handling.
- `0fe06c4`: background fanout failure must affect broker readiness.

Evidence: the corresponding ownership, session, protocol and lifecycle tests,
plus the 1,000-cycle fanout/lease test recorded in the Phase 4.6 baseline.

### Routing and memory density

- `d312454`, `0c1f108`, `e7ff6ff`, `84f6fdf`, `73ab908`, `867eb57`,
  `55289f9`, `98b7d42`: immutable route epochs, persistent HAMT, shared-group
  cursors, retained trie and shard-aware route planning.
- `3c5bac2`, `7dd3494`, `d82af22`: intrusive QoS 0 queue, single-Slab lease and
  lazy Topic Alias storage.
- `3e04ead`, `44ed9da`, `d7b6d54`, `21fad16`: shared-cursor stability, queue
  scan elimination and UTF-8 route indexing.

Evidence: routing, retained, queue and allocation tests; Phase 4 and Phase 3
baselines; all route planning and fanout microbenchmarks are zero-allocation.

### Protocol and packet handling

- `8f66e76`, `1cf0704`, `f1629fb`, `186692c`: single-segment properties,
  encoded-length caching, reserved PUBLISH headers and MQTT 5 reason-only ACKs.
- `74059c2`, `9e0a991`, `ed9af05`, `4d4b7fd`: ACK ordering, publish interceptor
  preparation and QoS acknowledgement correctness.

Evidence: protocol, property, queued-message and publish-codec tests.

### Benchmark infrastructure

- `146db96`, `7fe633d`, `449d5a6`, `7754ac6`, `b1a4c65`, `54c40be`,
  `0fab482`, `3e2efcd`, `ab5ee86`, `b170dee`, `654d87f`: separated processes,
  MQTTnet-on-Kestrel comparison, strict gate, raw client, observations,
  smoke/nightly workflow and matrix controls.
- `44d1654`, `56f7566`, `12b591e`, `9016738`, `da0ba89`, `ce6111f`,
  `bd6f232`: recorded baselines and rejected transport conclusion.

Evidence: benchmark unit tests, smoke tests and the raw JSON under
`perf/baseline`.

## Neutral

- `e2fab41`, `dfe9861`: batch and large-payload scheduling scaffolding retained
  only where a kept test or ownership rule depends on it. Do not retain a
  production fast path merely because it reduces scheduler passes.
- `76d54e`, `c582a1c`, `52f7bae`, `db55089`, `99bbbda`, `37b4593`,
  `a438682`, `4a580b8`: event-loop shard boundaries and stable session mapping.
  They are retained as the current runtime base, not as proof of end-to-end
  performance.
- `31644db`, `9362274`: internal transport boundary retained; the experimental
  Socket implementation is not retained.
- `e3fa472`, `0881f61`, `1fc780c`, `66d7bad`, `506d146`, `35bea63`: Egress
  lifecycle and control-frame ownership required by the current tests. The
  full send-loop ownership remains for the separate Egress closure experiment.
- `8330742`, `90da374`, `2b12c47`: diagnostics are retained only when they are
  useful for attribution and remain zero-allocation when disabled.

## Reject

- `f5e8736`, `25806d9`, `4d4b7fd`, `67d57b8`, `94be882`, `e81e638`,
  `8c86bbf`: scheduler/batch experiments whose only claimed benefit is a red
  performance cell or which are superseded by the next Egress experiment.
  Their tests remain useful only if they protect a kept lifecycle invariant.
- `a89d2e9`: rejected direct large-publish write strategy. The Phase 4.6
  baseline records a severe regression.
- `31644db`/`9362274` Socket selection code is neutral only; the prototype
  implementation was removed by `ce6111f`, which is retained as the decision
  record.
- Any code that enlarges a configured batch limit solely to improve one
  microbenchmark is rejected. The Phase 4.6 A/B result shows no stable win.

## Migration Rule

The release branch migrates the kept groups first, runs tests, and then adds
neutral boundary code needed by those groups. Rejected behavior is not copied
to `master`. The original research branch remains intact for evidence and for
the one-shot Egress closure experiment.
