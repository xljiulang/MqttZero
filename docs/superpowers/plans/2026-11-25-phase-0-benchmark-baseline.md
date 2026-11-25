# Phase 0 Benchmark Baseline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** 建立可重复、可归因、Windows/Linux 可执行的 MQTTZero 与 MQTTnet Broker 性能基线工具链。

**Architecture:** 保留现有 MQTTnet Client 正式门禁，新增原始 MQTT 5 QoS 0/1 诊断客户端；显式固定运行时配置，并记录环境、GC、ThreadPool、进程资源和阶段耗时。PR 只跑 smoke，夜间运行 18 单元格严格矩阵并上传 JSON。

**Tech Stack:** .NET 10、Benchmark Broker 控制台项目、xUnit v3、Microsoft Testing Platform、MQTTnet 5.2.0.1603、Kestrel、PowerShell 7、GitHub Actions。

**Spec:** `docs/sharded-actor-broker-plan.md` 的 Phase 0。

## Global Constraints

- 目标框架固定 `net10.0`，只支持 MQTT 5。
- Phase 0 不修改 `src/` 下的生产 Broker 实现。
- 正式比较继续使用 MQTTnet 5.2.0.1603 Client。
- 原始 MQTT Client 本轮只支持明文 TCP、QoS 0/1，不能替代正式门禁。
- Phase 0 本地开发矩阵固定 QoS `0/1`、订阅者 `1/100`、Payload `32/8192/65536`、预热 `2s`、测量 `6s`、重复 `1` 次。
- 夜间稳定矩阵使用 QoS `0/1/2`、订阅者 `1/100`、Payload `32/8192/65536`、预热 `5s`、测量 `20s`、重复 `3` 次。
- `1000` 订阅者场景从 Phase 0 开发基线中移除，保留到后续最终性能门禁重新启用。
- 本地开发基线固定预热 `2s`、测量 `6s`、重复 `1` 次。
- 夜间稳定基线固定预热 `5s`、测量 `20s`、重复 `3` 次。
- 每个单元格必须满足吞吐比 `> 1.00`、p99 比 `< 1.00`，且两个 Broker 的失败操作数均为 `0`。
- 容器和 Shard 数不是 Phase 0 验收项；100,000 连接验证继续延后。
- Benchmark Broker 必须位于独立子进程，日志关闭，预热排空后清零计数器。
- Windows 和 Linux 使用相同参数、相同运行时配置和同一份固定矩阵。
- C# 文件使用 CRLF；public 类型和方法必须有 XML 注释；一个文件只能有一个顶层类型；所有 if/方法体使用大括号。
- JSON 序列化使用 `JsonSerializerContext` 源生成。
- 所有命令使用 PowerShell 7；所有验证必须在当前工作树重新运行。

## File Structure

- `benchmarks/MqttZero.BrokerBenchmarks/`：Benchmark CLI、运行时配置、观测 DTO、客户端抽象和原始 MQTT Client。
- `tests/MqttZero.BrokerBenchmarks.Tests/`：Benchmark 配置、门禁、观测和原始客户端测试。
- `eng/`：smoke 与 baseline PowerShell 入口。
- `.github/workflows/`：PR smoke 与 Windows/Linux nightly performance。
- `perf/baseline/`：Phase 0 Windows 开发基线和说明。

---

### Task 1: Benchmark 测试项目与运行时配置

**Files:**
- Create: `tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj`
- Create: `tests/MqttZero.BrokerBenchmarks.Tests/BenchmarkRuntimeConfigurationTests.cs`
- Create: `tests/MqttZero.BrokerBenchmarks.Tests/TestAssembly.cs`
- Create: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkRuntimeConfiguration.cs`
- Create: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkRuntimeSnapshot.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/MqttZero.BrokerBenchmarks.csproj`
- Modify: `MqttZero.slnx`

**Interfaces:**
- Produces: `BenchmarkRuntimeConfiguration(int ThreadPoolMinimumWorkerThreads)`。
- Produces: `IReadOnlyDictionary<string,string> BenchmarkRuntimeConfiguration.CreateBrokerEnvironmentVariables()`。
- Produces: `BenchmarkRuntimeSnapshot BenchmarkRuntimeConfiguration.ApplyToCurrentProcess()`。
- Produces: `BenchmarkRuntimeSnapshot`，包含 Server GC、Concurrent GC、TieredPGO、最小/最大 Worker 和 IO 线程。

- [x] **Step 1: 创建测试项目**

使用 xUnit v3/Microsoft Testing Platform，添加对 Benchmark 项目的引用，并启用串行测试。

- [x] **Step 2: 写失败测试**

```csharp
[Fact]
public void BrokerEnvironmentVariablesUseExplicitRuntimeConfiguration()
{
    BenchmarkRuntimeConfiguration configuration = new(8);

    IReadOnlyDictionary<string, string> environment =
        configuration.CreateBrokerEnvironmentVariables();

    Assert.Equal("1", environment["DOTNET_gcServer"]);
    Assert.Equal("1", environment["DOTNET_gcConcurrent"]);
    Assert.Equal("1", environment["DOTNET_TieredPGO"]);
    Assert.Equal("8", environment["DOTNET_ThreadPool_ForceMinWorkerThreads"]);
}
```

- [x] **Step 3: 运行并确认失败**

```powershell
dotnet test tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release
```

Expected: 编译失败，提示 `BenchmarkRuntimeConfiguration` 不存在。

- [x] **Step 4: 实现最小配置类型**

构造参数小于 `1` 时抛出 `ArgumentOutOfRangeException`。`ApplyToCurrentProcess()` 使用 `ThreadPool.GetMinThreads`、`ThreadPool.SetMinThreads`，记录修改后的有效值；禁止降低当前进程已有的最小线程数。

- [x] **Step 5: 固定 Benchmark runtimeconfig**

在 Benchmark csproj 添加：

```xml
<ItemGroup>
  <RuntimeHostConfigurationOption Include="System.GC.Server" Value="true" />
  <RuntimeHostConfigurationOption Include="System.GC.Concurrent" Value="true" />
  <RuntimeHostConfigurationOption Include="System.Runtime.TieredPGO" Value="true" />
</ItemGroup>
```

为测试程序集添加 `InternalsVisibleTo Include="MqttZero.BrokerBenchmarks.Tests"`。

- [x] **Step 6: 验证**

```powershell
dotnet restore MqttZero.slnx --locked-mode
dotnet build MqttZero.slnx -c Release --no-restore
dotnet test tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release --no-build
```

- [x] **Step 7: 提交**

```powershell
git add MqttZero.slnx benchmarks/MqttZero.BrokerBenchmarks tests/MqttZero.BrokerBenchmarks.Tests
git commit -m "perf: configure broker benchmark runtime"
```

---

### Task 2: 环境、进程、GC 与 ThreadPool 观测

**Files:**
- Create: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkEnvironmentInfo.cs`
- Create: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkProcessObservation.cs`
- Create: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkLoadObservation.cs`
- Create: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkObservationCapture.cs`
- Create: `tests/MqttZero.BrokerBenchmarks.Tests/BenchmarkObservationCaptureTests.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/BrokerBenchmarkResult.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/MqttZero.BrokerBenchmarks.csproj`

**Interfaces:**
- Produces: `BenchmarkEnvironmentInfo BenchmarkObservationCapture.CaptureEnvironment(BenchmarkRuntimeSnapshot runtime)`。
- Produces: `BenchmarkProcessObservation BenchmarkObservationCapture.CaptureProcess(Process process, TimeSpan uptime)`。
- Produces: `BenchmarkLoadObservation BenchmarkObservationCapture.CaptureLoadDelta(BenchmarkLoadSnapshot before)`。
- Produces: `BenchmarkLoadSnapshot BenchmarkObservationCapture.CreateLoadSnapshot()`。

- [x] **Step 1: 写失败测试**

测试环境字段非空、ProcessorCount 大于零，并验证 Snapshot 中 Runtime 与输入一致。

- [x] **Step 2: 添加计数器测试**

分配一个至少 4096 字节的数组，验证 CPU、Working Set、Gen0/1/2、Allocated 和 ThreadPool Pending/Completed 增量均为非负。

- [x] **Step 3: 运行并确认失败**

```powershell
dotnet test tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release --filter "FullyQualifiedName~BenchmarkObservationCaptureTests"
```

- [x] **Step 4: 实现观测**

`BenchmarkProcessObservation` 包含 Broker CPU 毫秒、PrivateMemoryBytes、PeakWorkingSetBytes 和 UptimeMilliseconds。`BenchmarkLoadObservation` 包含 Load Generator CPU、Working Set、Gen0/1/2、Allocated Bytes、Pending 和 Completed ThreadPool WorkItem 增量。

- [x] **Step 5: 扩展结果模型**

`BrokerBenchmarkResult` 新增 `LoadObservation`、`ConnectionSetupMilliseconds`、`WarmupMilliseconds`、`MeasurementMilliseconds` 和 `DrainMilliseconds`。阶段时间使用 `Stopwatch.GetTimestamp()`。

- [x] **Step 6: 验证并提交**

```powershell
dotnet build MqttZero.slnx -c Release --no-restore
dotnet test tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release --no-build
git add benchmarks/MqttZero.BrokerBenchmarks tests/MqttZero.BrokerBenchmarks.Tests
git commit -m "perf: capture broker benchmark observations"
```

---

### Task 3: 严格门禁、结果汇总与源生成 JSON

**Files:**
- Create: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkGateEvaluator.cs`
- Create: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkSummary.cs`
- Create: `benchmarks/MqttZero.BrokerBenchmarks/BrokerBenchmarkJsonContext.cs`
- Create: `tests/MqttZero.BrokerBenchmarks.Tests/BenchmarkGateEvaluatorTests.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/BrokerComparisonResult.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/Program.cs`

**Interfaces:**
- Produces: `BenchmarkGateResult BenchmarkGateEvaluator.Evaluate(IReadOnlyList<BrokerComparisonResult> results)`。
- Produces: `BenchmarkGateResult.Summaries` and `BenchmarkGateResult.Passed`。
- Produces: `BrokerBenchmarkJsonContext`。

- [x] **Step 1: 写失败测试**

验证相同吞吐和 p99 时严格门禁失败；任一 Broker 有失败操作时门禁失败。

- [x] **Step 2: 运行并确认失败**

```powershell
dotnet test tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release --filter "FullyQualifiedName~BenchmarkGateEvaluatorTests"
```

- [x] **Step 3: 实现严格门禁**

```csharp
cellPassed =
    mqttZeroFailures == 0 &&
    mqttNetFailures == 0 &&
    throughputRatio > 1.0 &&
    p99Ratio < 1.0;
```

缺少 Broker 结果、除数为零或重复数量不一致时必须失败并记录原因。

- [x] **Step 4: 替换匿名 JSON 汇总**

所有输出通过 `BrokerBenchmarkJsonContext.Default` 序列化，保持缩进。

- [x] **Step 5: 验证并提交**

```powershell
dotnet test tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release
dotnet run -c Release --project benchmarks/MqttZero.BrokerBenchmarks -- compare --qos 0 --subscribers 1 --payload 32 --warmup 1 --seconds 2 --repetitions 1
git add benchmarks/MqttZero.BrokerBenchmarks tests/MqttZero.BrokerBenchmarks.Tests
git commit -m "perf: enforce strict broker benchmark gate"
```

---

### Task 4: Benchmark Client 抽象与原始 MQTT 5 客户端

**Files:**
- Create: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkClientKind.cs`
- Create: `benchmarks/MqttZero.BrokerBenchmarks/IBenchmarkMqttClient.cs`
- Create: `benchmarks/MqttZero.BrokerBenchmarks/IBenchmarkMqttClientFactory.cs`
- Create: `benchmarks/MqttZero.BrokerBenchmarks/MqttNetBenchmarkMqttClient.cs`
- Create: `benchmarks/MqttZero.BrokerBenchmarks/MqttNetBenchmarkMqttClientFactory.cs`
- Create: `benchmarks/MqttZero.BrokerBenchmarks/RawMqttClientOptions.cs`
- Create: `benchmarks/MqttZero.BrokerBenchmarks/RawMqttClient.cs`
- Create: `tests/MqttZero.BrokerBenchmarks.Tests/RawMqttClientTests.cs`
- Create: `tests/MqttZero.BrokerBenchmarks.Tests/TestBrokerHost.cs`

**Interfaces:**
- Produces: `BenchmarkClientKind { MqttNet, Raw }`。
- Produces: `IBenchmarkMqttClient`。
- Produces: `IBenchmarkMqttClientFactory`。
- Produces: `RawMqttClient`，支持明文 TCP MQTT 5 QoS 0/1。

- [x] **Step 1: 写原始客户端集成测试**

启动本地 MQTTZero Broker，创建 Raw subscriber 和 publisher，完成 QoS1 订阅、发布和接收。

- [x] **Step 2: 写不支持 QoS 2 的测试**

Raw Client 在写入网络前抛出 `NotSupportedException`。

- [x] **Step 3: 运行并确认失败**

```powershell
dotnet test tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release --filter "FullyQualifiedName~RawMqttClientTests"
```

- [x] **Step 4: 实现 MQTTnet Client 适配器**

保持连接、订阅、QoS 0/1/2 发布、接收计数、断线和释放语义。

- [x] **Step 5: 实现原始客户端**

使用 `TcpClient`、`PipeReader.Create`、`PipeWriter.Create` 和公开 `MqttPacketCodec`。支持 CONNECT/CONNACK、SUBSCRIBE/SUBACK、PUBLISH QoS 0/1、PUBACK、接收计数和 DISCONNECT。写入通过单写入锁串行化；Packet Identifier 循环复用前检查未完成请求。

- [x] **Step 6: 验证并提交**

```powershell
dotnet build MqttZero.slnx -c Release --no-restore
dotnet test tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release --no-build
git add benchmarks/MqttZero.BrokerBenchmarks tests/MqttZero.BrokerBenchmarks.Tests
git commit -m "perf: add raw mqtt benchmark client"
```

---

### Task 5: Runner、阶段计时与 `--client` 选择

**Files:**
- Create: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkLoadRunner.cs`
- Create: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkOutputWriter.cs`
- Create: `tests/MqttZero.BrokerBenchmarks.Tests/BenchmarkClientSelectionTests.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/Program.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/BrokerBenchmarkResult.cs`

**Interfaces:**
- Consumes: `IBenchmarkMqttClientFactory` 和 `BrokerBenchmarkResult`。
- Produces: `BenchmarkLoadRunner.RunAsync(...)`。
- Produces: `BenchmarkOutputWriter.WriteAsync<T>(...)`。
- CLI 新增 `--client mqttnet|raw` 和 `--output <path>`。

- [x] **Step 1: 写失败测试**

验证 Client Kind 大小写不敏感解析和未知值拒绝。

- [x] **Step 2: 运行并确认失败**

```powershell
dotnet test tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release --filter "FullyQualifiedName~BenchmarkClientSelectionTests"
```

- [x] **Step 3: 提取 Runner**

把连接、预热、排空、测量和清理移入 `BenchmarkLoadRunner`，保持闭环上限和阶段顺序。

- [x] **Step 4: 接入原始客户端**

`--client raw` 遇到 QoS 2 时在启动 Broker 前失败。默认仍为 `mqttnet`。

- [x] **Step 5: 输出 Broker 进程观测**

子进程启动前记录时间；READY 后计算启动耗时；负载结束且终止进程前采集观测。

- [x] **Step 6: 验证并提交**

```powershell
dotnet run -c Release --no-build --project benchmarks/MqttZero.BrokerBenchmarks -- compare --client mqttnet --qos 0,1 --subscribers 1 --payload 32,8192 --warmup 1 --seconds 2 --repetitions 1 --start-port 24000
dotnet run -c Release --no-build --project benchmarks/MqttZero.BrokerBenchmarks -- compare --client raw --qos 0,1 --subscribers 1 --payload 32,8192 --warmup 1 --seconds 2 --repetitions 1 --start-port 24500
git add benchmarks/MqttZero.BrokerBenchmarks tests/MqttZero.BrokerBenchmarks.Tests
git commit -m "perf: add raw benchmark runner and stage timings"
```

---

### Task 6: PR Smoke 与 Windows/Linux Nightly Workflow

**Files:**
- Create: `eng/verify-benchmark-smoke.ps1`
- Create: `eng/run-broker-benchmark.ps1`
- Create: `.github/workflows/performance.yml`
- Modify: `.github/workflows/ci.yml`

- [x] **Step 1: 写 smoke 脚本**

运行 MQTTnet 和 Raw Client 的 QoS 0/1 smoke，解析 JSON，确认两个 Broker 均存在且失败数为 0；忽略性能比值。

- [x] **Step 2: 写正式 Benchmark 脚本**

本地入口默认执行 12 单元格开发矩阵；夜间 workflow 显式覆盖为 18 单元格稳定矩阵，保存 JSON 后透传严格门禁退出码。

- [x] **Step 3: 添加 PR smoke**

在 `.github/workflows/ci.yml` 的 Windows/Ubuntu Broker job 中执行 `./eng/verify-benchmark-smoke.ps1`。

- [x] **Step 4: 添加 nightly workflow**

`.github/workflows/performance.yml` 使用每日 `18:00 UTC`、`workflow_dispatch`、Windows/Ubuntu 矩阵、360 分钟 timeout、artifact 上传且 `if: always()`。严格门禁失败时 job 失败。

- [x] **Step 5: 验证并提交**

```powershell
./eng/verify-benchmark-smoke.ps1
./eng/run-broker-benchmark.ps1 -Client raw -OutputPath artifacts/raw-smoke.json -StartPort 31000
git add .github/workflows eng
git commit -m "perf: add benchmark smoke and nightly workflows"
```

---

### Task 7: Windows 开发基线与 Linux 证据入口

**Files:**
- Create: `perf/baseline/broker-phase0-2026-11-25.json`
- Create: `perf/baseline/broker-phase0-2026-11-25.md`
- Modify: `perf/baseline/README.md`

- [x] **Step 1: 运行 Windows 开发基线**

```powershell
./eng/run-broker-benchmark.ps1 -Client mqttnet -OutputPath perf/baseline/broker-phase0-2026-11-25.json -StartPort 30000
```

允许严格门禁未通过而返回非零，但必须确认 JSON 完整生成。

- [x] **Step 2: 运行原始客户端诊断**

```powershell
./eng/run-broker-benchmark.ps1 -Client raw -OutputPath artifacts/broker-phase0-raw-2026-11-25.json -StartPort 32000
```

- [x] **Step 3: 写 Markdown 基线**

记录环境、运行时配置、12 单元格开发结果、失败数、CPU、Working Set、GC、ThreadPool、原始客户端对比、未通过单元格和 Linux pending 状态。

- [x] **Step 4: 更新 baseline 索引**

在 `perf/baseline/README.md` 添加 Phase 0 文件，并说明测试工具链成功不等于性能门禁通过。

- [x] **Step 5: 全量验证**

```powershell
dotnet restore MqttZero.slnx --locked-mode
dotnet build MqttZero.slnx -c Release --no-restore
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-build
dotnet test tests/MqttZero.Dashboard.Tests/MqttZero.Dashboard.Tests.csproj -c Release --no-build
dotnet test tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release --no-build
./perf/verify-allocation.ps1
git diff --check
```

- [x] **Step 6: 提交**

```powershell
git add perf/baseline
git commit -m "docs: record phase zero broker baseline"
```

## Phase 0 Acceptance

- Benchmark 测试项目与全部新测试通过。
- MQTTnet Client 的本地开发矩阵为 12 单元格，夜间稳定矩阵为 18 单元格；1000 订阅者最终门禁延后。
- Raw MQTT 5 Client 可完成明文 TCP QoS 0/1 连接、订阅、发布、PUBACK 和接收统计。
- 输出包含环境、Broker/Load 观测、阶段耗时、失败数和严格门禁。
- PR smoke 在 Windows/Linux 运行且不因性能波动误报。
- Nightly workflow 在 Windows/Linux 执行 18 单元格稳定矩阵，严格失败并上传 JSON。
- Windows 开发 2×6 秒、单次基线 artifact 和 Markdown 报告存在。
- 当前分支若尚未优于 MQTTnet，Phase 0 仍可完成，但必须明确记录红色门禁，不能宣称性能目标通过。

## Assumptions

- Linux 稳定基线需要 nightly workflow 在环境实际运行后才能补齐；离线执行时标记 `pending`。
- 原始客户端只用于诊断 QoS 0/1，不作为正式性能门禁。
- GitHub-hosted runner 只用于建立自动化入口；正式容量声明后续仍需专用同机或同规格性能机。
- Phase 0 不修改生产 Broker，也不开始 Event Loop Shard、Egress Actor 或路由重构。
