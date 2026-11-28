# MQTTZero Linux 开发服务器 MQTTnet 性能路线图实施计划

> **致执行代理：** 必须使用子技能 `superpowers:subagent-driven-development`（推荐）或 `superpowers:executing-plans`，按任务逐项执行本计划。使用 `- [ ]` 复选框跟踪进度。

**目标：** 在稳定的 Linux 开发服务器上建立唯一的正式 MQTTnet 对比环境，先补齐可信的主机指纹、资源观测和门禁口径，再依据数据逐项优化 MQTTZero。

**架构：** Linux 开发服务器是唯一的正式性能环境。开发用本机可能在家中和公司之间切换，因此只用于编译、协议测试、功能回归和可丢弃的快速诊断，不作为发布性能证据。正式对比必须在同一台 Linux 开发服务器、同一次测试活动内，以交替顺序分别启动 MQTTZero 和 MQTTnet，禁止拼接不同主机或不同日期的数据。

**技术栈：** .NET 10、ASP.NET Core Kestrel、MQTT 5、eMQTT-Bench 0.6.4、Python 3、Bash、PowerShell 7、MQTTnet 5.2.0.1603

**规格来源：** `perf/baseline/broker-comparison-methodology.md`

## 全局约束

- 外部性能对比目标固定为 MQTTnet Server。
- 外部负载客户端固定为 eMQTT-Bench `0.6.4`。
- Linux 开发服务器是正式性能环境，因为它在不同开发地点之间保持稳定。
- 禁止同时在 Linux 开发服务器和开发用本机上运行正式负载测试。
- 禁止合并来自不同主机、操作系统、本地 Linux 环境、日期或电源模式的结果。
- 即使开发用本机的物理核数更多，其结果也不能作为发布证据。
- Linux 开发服务器正式测试默认最多使用 `500` 个订阅者；`1000` 订阅者只在完整矩阵明确需要时启用。
- Linux 开发服务器文件描述符软、硬上限已提升并验证为 `1048576`。
- 每个正式单元格都必须在同一轮测试中分别运行 MQTTZero 和 MQTTnet，并采用交替顺序。
- 日期、代码提交、主机状态或运行环境发生变化后，必须重新运行 MQTTZero 和 MQTTnet 两侧。
- 吞吐档门禁使用端到端投递吞吐和投递完整性；该档位的 p95/p99 记录为积压完成时间诊断，不描述成正常负载尾延迟。
- 延迟档使用限速发布，只有 QoE 覆盖率至少达到 `99%` 时，才允许使用 p99 门禁。
- eMQTT-Bench 0.6.4 在 2 ms 间隔下仍无法完整投递 `QoS0 / 64 KiB`，因此该组合不进入正式矩阵；64 KiB 外部对比使用 QoS1。
- eMQTT-Bench 0.6.4 的 QoS0 在高扇出下仍会出现投递缺口，因此正式跨 Broker 吞吐门禁固定使用 QoS1；QoS0 只保留单订阅/小扇出冒烟和内部 Raw 诊断，不产生 MQTTnet 性能结论。
- 高扇出单元格按订阅者数缩减发布条数：32 B 为 `5000/1000/200`，8 KiB 为 `1000/200/50`，64 KiB 固定 `100`，分别对应 `1/100/500` 订阅者，避免在 2 vCPU 开发机上单格产生数百万次投递。
- 开发回归使用两轮。仅当吞吐离散超过 `10%`、运行无效或投递不完整时，补跑第三轮。
- 正式性能证据使用三轮，并分别计算两个 Broker 的中位数。
- 本路线图不修改公开 API、MQTT wire 行为、Dashboard 契约或配置默认值。
- 在诊断确认瓶颈前，不修改批次大小、缓冲区预算或调度器结构。
- 热路径修改必须保持零分配哨兵，并在取消、Stop、Detach 和 Takeover 路径中保证资源只释放一次。
- 纯后端基准或 Broker 修改不要求运行 Dashboard 和 Vue 验证。

---

## 固定测试环境

| 环境 | 用途 | 已知配置 | 证据级别 |
|---|---|---|---|
| `linux-dev-2vcpu` | 唯一正式性能环境 | AMD EPYC 7K62，2 vCPU，3.6 GiB 内存，Ubuntu 24.04，.NET 10.0.115，KVM，无 SMT，文件描述符上限 1048576 | 开发级正式证据，不作为容量声明 |
| `developer-laptop` | 编译、正确性测试和临时诊断 | Windows 和本地 Linux 环境，可能在家中和公司之间变化 | 永远不能作为发布性能门禁 |

Linux 开发服务器固定路径：

```text
工作目录：/home/ubuntu/mqttzero-dev
Broker：/home/ubuntu/mqttzero-dev/artifacts/broker-linux-x64/current/runtime
客户端：/home/ubuntu/mqttzero-dev/tools/emqtt-bench/0.6.4/bin/emqtt_bench
```

开发用本机可以发起远程测试并查看结果，但不得为正式运行贡献 CPU、内存、网络负载或结果行。

SSH 配置路径由执行环境变量 `MQTTZERO_SSH_CONFIG` 提供，计划文件不记录具体机器目录名。

## 性能测试口径

### 快速循环

使用 `-Quick` 只运行一个目标格、一轮，并把 QoE Flush 等待降到 1 秒：

```text
QoS 1 / 500 订阅者 / 64 KiB
每个 Broker 运行 1 轮，交替顺序
通常数分钟内结束
不得产生发布声明
不得用于 p99 结论
```

### 日常门禁

每次普通改动选择一个主目标格，并附加最多两个保护格，各运行一轮：

```text
QoS 1 / 500 订阅者 / 64 KiB
QoS 1 / 100 订阅者 / 8 KiB
QoS 1 / 1 订阅者 / 64 KiB
```

只有里程碑或正式证据才扩展到两轮或三轮完整矩阵。

### 正式门禁

使用三轮正式测试。

吞吐档：

```text
端到端投递吞吐 >= MQTTnet * 1.00
receivedDeliveries == publishedMessages * subscribers
失败、缺失、重复、乱序和排空超时必须为 0
```

延迟档：

```text
QoE 覆盖率 >= 99%
投递 p99 <= MQTTnet p99
```

p95 用于更早发现开发回归：

```text
吞吐档：同时记录 p50/p95/p99 的积压完成时间
延迟档：p95 先于 MQTTnet 失败时提前停下，但正式尾延迟门禁仍使用 p99
```

不能通过增加“同等延迟但未被 p99 覆盖”的消息来优化 p95。

## 结果身份

每个结果目录都必须包含 `host.json`：

```json
{
  "hostId": "linux-dev-2vcpu",
  "commit": "4f8f60b2b0f5f28cd51f7ce3c5d5e79c686b90d0",
  "cpuModel": "AMD EPYC 7K62 48-Core Processor",
  "logicalCpuCount": 2,
  "memoryBytes": 3904782336,
  "os": "Ubuntu 24.04",
  "dotnetVersion": "10.0.115",
  "emqttVersion": "0.6.4",
  "fileDescriptorLimit": 1048576,
  "powerMode": "n/a"
}
```

缺少 `hostId`、提交、CPU 数、内存、操作系统、dotnet 版本或 eMQTT 版本的结果一律无效。

---

### 任务 1：增加 Linux 开发服务器指纹和预检

**文件：**
- 新建：`eng/emqtt-bench/host_preflight.py`
- 新建：`eng/emqtt-bench/tests/test_host_preflight.py`
- 修改：`eng/emqtt-bench/run-cell.sh`
- 修改：`eng/emqtt-bench/README.md`

**接口：**
- 输入：Linux `/proc/cpuinfo`、`/proc/meminfo`、`ulimit -n`、`dotnet --version`、`git rev-parse HEAD`
- 输出：`host.json`，包含 `hostId`、`commit`、`cpuModel`、`logicalCpuCount`、`memoryBytes`、`os`、`dotnetVersion`、`emqttVersion`、`fileDescriptorLimit`

- [x] **步骤 1：先写失败的解析测试**

创建 `eng/emqtt-bench/tests/test_host_preflight.py`：

```python
import unittest

from host_preflight import (
    parse_cpu_model,
    parse_logical_cpu_count,
    parse_memory_bytes,
    validate_profile,
)


class HostPreflightTests(unittest.TestCase):
    def test_parses_proc_cpu_model_and_count(self) -> None:
        text = """
processor   : 0
model name  : AMD EPYC 7K62 48-Core Processor
processor   : 1
model name  : AMD EPYC 7K62 48-Core Processor
"""
        self.assertEqual(
            "AMD EPYC 7K62 48-Core Processor",
            parse_cpu_model(text),
        )
        self.assertEqual(2, parse_logical_cpu_count(text))

    def test_parses_mem_total(self) -> None:
        self.assertEqual(
            2 * 1024 * 1024 * 1024,
            parse_memory_bytes("MemTotal:        2097152 kB\n"),
        )

    def test_rejects_mixed_hosts(self) -> None:
        with self.assertRaises(ValueError):
            validate_profile(
                {
                    "hostId": "linux-dev-2vcpu",
                    "logicalCpuCount": 2,
                },
                {
                    "hostId": "developer-laptop",
                    "logicalCpuCount": 8,
                },
            )
```

- [x] **步骤 2：运行测试并确认失败**

运行：

```powershell
py -3 -m unittest discover -s eng/emqtt-bench/tests -p "test_host_preflight.py" -v
```

预期：因为 `host_preflight.py` 不存在而导入失败。

- [x] **步骤 3：实现预检工具**

创建 `eng/emqtt-bench/host_preflight.py`，实现测试中的函数和以下命令行入口：

```python
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host-id", required=True)
    parser.add_argument("--expected-cpus", required=True, type=int)
    parser.add_argument("--expected-min-memory-gib", required=True, type=int)
    parser.add_argument("--expected-fd-limit", required=True, type=int)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    profile = collect_profile(args.host_id, args.expected_fd_limit)
    if profile["logicalCpuCount"] != args.expected_cpus:
        raise SystemExit("逻辑 CPU 数与批准的主机配置不一致")
    if profile["memoryBytes"] < args.expected_min_memory_gib * 1024**3:
        raise SystemExit("内存低于批准的主机最低值")
    write_json(Path(args.output), profile)
```

- [x] **步骤 4：运行解析测试**

运行：

```powershell
py -3 -m unittest discover -s eng/emqtt-bench/tests -p "test_host_preflight.py" -v
```

预期：全部通过。

- [x] **步骤 5：强制 `run-cell.sh` 校验主机指纹**

在脚本开头增加：

```bash
host_profile=${HOST_PROFILE:?必须通过 HOST_PROFILE 指定已批准的 host.json}
```

启动 Broker 前调用：

```bash
python3 "$script_dir/host_preflight.py" \
    --validate "$host_profile" \
    --current "$output_dir/current-host.json"
```

如果 `hostId`、逻辑 CPU 数、操作系统、内存或文件描述符上限不一致，必须失败退出。

- [x] **步骤 6：在 Linux 开发服务器验证**

运行：

```bash
cd /home/ubuntu/mqttzero-dev/eng/emqtt-bench
python3 host_preflight.py \
  --host-id linux-dev-2vcpu \
  --expected-cpus 2 \
  --expected-min-memory-gib 3 \
  --expected-fd-limit 1048576 \
  --output /tmp/linux-dev-host.json
cat /tmp/linux-dev-host.json
```

预期：退出码为 `0`，CPU 数为 `2`，内存至少 `3 GiB`，文件描述符上限为 `1048576`。

- [x] **步骤 7：提交**

```bash
git add eng/emqtt-bench/host_preflight.py \
  eng/emqtt-bench/tests/test_host_preflight.py \
  eng/emqtt-bench/run-cell.sh \
  eng/emqtt-bench/README.md
git commit -m "bench: fingerprint the approved performance host"
```

---

### 任务 2：增加 Linux 开发服务器远程测试入口

**文件：**
- 新建：`eng/emqtt-bench/run-linux-dev.ps1`
- 新建：`eng/emqtt-bench/tests/test-run-linux-dev-contract.ps1`
- 修改：`eng/emqtt-bench/README.md`

**接口：**
- 输入：SSH 配置、当前 Git 提交、矩阵模式、QoS、订阅者、Payload、重复次数
- 输出：复制回开发用本机的 `artifacts/emqtt-bench/${Campaign}/`，其中包含 `host.json`、`runs.tsv`、原始结果和 `summary.md`

- [x] **步骤 1：写远程测试入口的失败契约测试**

创建 `eng/emqtt-bench/tests/test-run-linux-dev-contract.ps1`：

```powershell
$ErrorActionPreference = 'Stop'

$output = & ./eng/emqtt-bench/run-linux-dev.ps1 -DryRun `
    -Mode throughput `
    -Qos 1 `
    -Subscribers 500 `
    -Payload 65536 `
    -Repetitions 1

if ($LASTEXITCODE -ne 0) {
    throw "Dry run failed."
}

$required = @(
    'hostId=linux-dev-2vcpu',
    'mode=throughput',
    'qos=1',
    'subscribers=500',
    'payload=65536',
    'repetitions=1'
)

foreach ($value in $required) {
    if ($output -notcontains $value) {
        throw "缺少 dry-run 输出：$value"
    }
}
```

- [x] **步骤 2：运行测试并确认失败**

运行：

```powershell
pwsh -NoProfile -File eng/emqtt-bench/tests/test-run-linux-dev-contract.ps1
```

预期：因为 `run-linux-dev.ps1` 不存在而失败。

- [x] **步骤 3：实现远程测试入口**

`run-linux-dev.ps1` 必须完成以下步骤：

```text
1. 通过 -SshConfigPath 读取 SSH 配置。
2. 要求工作树干净，并记录当前提交 SHA。
3. 只导出当前提交的 git archive。
4. 上传到 Linux 开发服务器固定活动目录。
5. 在 Linux 开发服务器内执行锁定恢复和 Release 构建。
6. 发布 `UseAppHost=false` 的便携式框架依赖 Broker Benchmark，并由服务器 .NET 10 运行。
7. 运行 host_preflight.py 并生成 host.json。
8. 调用 run-matrix.sh。
9. 把结果复制回 artifacts/emqtt-bench/${Campaign}。
10. 任一运行无效或测试期间主机指纹变化时失败。
```

示例：

```powershell
./eng/emqtt-bench/run-linux-dev.ps1 `
  -SshConfigPath $env:MQTTZERO_SSH_CONFIG `
  -Mode throughput `
  -Qos 1 `
  -Subscribers 500 `
  -Payload 65536 `
  -Repetitions 3 `
  -StartPort 19000
```

脚本只能启动 Linux 开发服务器上的 Broker 和客户端，不得让开发用本机生成负载。

- [x] **步骤 4：运行契约测试**

运行：

```powershell
pwsh -NoProfile -File eng/emqtt-bench/tests/test-run-linux-dev-contract.ps1
```

预期：通过。

- [x] **步骤 5：运行一次远程冒烟**

运行：

```powershell
./eng/emqtt-bench/run-linux-dev.ps1 `
  -SshConfigPath $env:MQTTZERO_SSH_CONFIG `
  -Mode latency `
  -Qos 1 `
  -Subscribers 1 `
  -Payload 8192 `
  -Repetitions 1 `
  -StartPort 19100 `
  -SmokeMessages 10
```

预期：投递完整、主机指纹有效、开发用本机没有启动负载进程。

- [x] **步骤 6：提交**

```bash
git add eng/emqtt-bench/run-linux-dev.ps1 \
  eng/emqtt-bench/tests/test-run-linux-dev-contract.ps1 \
  eng/emqtt-bench/README.md
git commit -m "bench: run mqttnet campaigns on the linux dev server"
```

---

### 任务 3：拆分吞吐门禁和尾延迟门禁

**文件：**
- 修改：`eng/emqtt-bench/summarize-matrix.py`
- 修改：`eng/emqtt-bench/tests/test_summarize_matrix.py`
- 修改：`eng/emqtt-bench/README.md`

**接口：**
- 输入：矩阵 `runs.tsv` 和每个运行的 `summary.json`
- 输出：`p95Ratio`、`p99Ratio`、`throughputGatePassed`、`tailGatePassed`、`gatePassed`

- [x] **步骤 1：先增加失败测试**

增加：

```python
def test_throughput_cell_records_percentiles_but_gates_only_throughput() -> None:
    cell = summarize_cell(
        "throughput",
        1,
        500,
        65536,
        3,
        {
            "mqttzero": [result(delivery=3900, p95=10000, p99=11000)] * 3,
            "mqttnet": [result(delivery=4300, p95=9000, p99=10000)] * 3,
        },
    )
    self.assertFalse(cell["throughputGatePassed"])
    self.assertAlmostEqual(1.111, cell["p95Ratio"], places=3)
    self.assertFalse(cell["latencyGateEligible"])


def test_latency_cell_uses_p99_for_formal_tail_gate() -> None:
    cell = summarize_cell(
        "latency",
        1,
        1,
        8192,
        3,
        {
            "mqttzero": [result(delivery=100, p95=3, p99=4)] * 3,
            "mqttnet": [result(delivery=100, p95=3, p99=4)] * 3,
        },
    )
    self.assertTrue(cell["latencyGateEligible"])
    self.assertTrue(cell["tailGatePassed"])
```

- [x] **步骤 2：运行测试并确认失败**

运行：

```powershell
py -3 -m unittest eng.emqtt-bench.tests.test_summarize_matrix -v
```

预期：字段缺失或函数签名不匹配。

- [x] **步骤 3：实现门禁拆分**

吞吐档：

```python
throughput_gate_passed = (
    complete
    and delivery_ratio is not None
    and delivery_ratio >= 1.0
)
tail_gate_passed = None
latency_gate_eligible = False
```

延迟档：

```python
latency_gate_eligible = latency_complete
throughput_gate_passed = None
tail_gate_passed = (
    latency_complete
    and p99_ratio is not None
    and p99_ratio <= 1.0
)
```

在 JSON 和 Markdown 中增加 p95 比值，列名为 `P95 ratio (diagnostic)`。

- [x] **步骤 4：运行矩阵汇总测试**

运行：

```powershell
py -3 -m unittest eng.emqtt-bench.tests.test_summarize_matrix -v
```

预期：全部通过。

- [x] **步骤 5：提交**

```bash
git add eng/emqtt-bench/summarize-matrix.py \
  eng/emqtt-bench/tests/test_summarize_matrix.py \
  eng/emqtt-bench/README.md
git commit -m "bench: separate throughput and tail latency gates"
```

---

### 任务 4：开放基准专用调参参数

**文件：**
- 修改：`eng/emqtt-bench/run-cell.sh`
- 修改：`eng/emqtt-bench/run-matrix.sh`
- 修改：`eng/emqtt-bench/tests/test_summarize_matrix.py`
- 修改：`eng/emqtt-bench/README.md`

**接口：**
- 输入：可选环境变量
- 输出：写入 `benchmark-config.json` 的实际 Broker 参数

支持以下环境变量：

```text
MAX_BUFFERED_BYTES
OUTBOUND_BATCH_BYTES
OUTBOUND_BATCH_MESSAGES
OUTBOUND_BATCH_DURATION_US
EVENT_LOOP_SHARDS
IO_QUEUE_COUNT
ENABLE_FANOUT_DIAGNOSTICS
ENABLE_MQTTZERO_MEMORY_PROBE
```

- [x] **步骤 1：为 `run-cell.sh` 增加参数打印模式**

启动 Broker 前增加：

```bash
if [[ "${PRINT_BROKER_ARGS:-0}" == "1" ]]; then
    printf '%s\n' "${broker_arguments[@]}"
    exit 0
fi
```

- [x] **步骤 2：追加 MQTTZero 调参参数**

```bash
if [[ -n "${MAX_BUFFERED_BYTES:-}" ]]; then
    broker_arguments+=(--max-buffered-bytes "$MAX_BUFFERED_BYTES")
fi
if [[ -n "${OUTBOUND_BATCH_BYTES:-}" ]]; then
    broker_arguments+=(--outbound-batch-bytes "$OUTBOUND_BATCH_BYTES")
fi
if [[ -n "${OUTBOUND_BATCH_MESSAGES:-}" ]]; then
    broker_arguments+=(--outbound-batch-messages "$OUTBOUND_BATCH_MESSAGES")
fi
if [[ -n "${OUTBOUND_BATCH_DURATION_US:-}" ]]; then
    broker_arguments+=(--outbound-batch-duration-us "$OUTBOUND_BATCH_DURATION_US")
fi
if [[ -n "${EVENT_LOOP_SHARDS:-}" ]]; then
    broker_arguments+=(--event-loop-shards "$EVENT_LOOP_SHARDS")
fi
if [[ -n "${IO_QUEUE_COUNT:-}" ]]; then
    broker_arguments+=(--io-queue-count "$IO_QUEUE_COUNT")
fi
```

- [x] **步骤 3：验证参数输出**

运行：

```bash
MAX_BUFFERED_BYTES=134217728 \
OUTBOUND_BATCH_MESSAGES=4 \
PRINT_BROKER_ARGS=1 \
./eng/emqtt-bench/run-cell.sh mqttzero 19001 1 500 65536 100 10 /tmp/args
```

预期输出包含：

```text
serve
--broker
mqttzero
--max-buffered-bytes
134217728
--outbound-batch-messages
4
```

不得启动 Broker。

- [x] **步骤 4：保存实际参数**

启动 Broker 前写入 `$output_dir/benchmark-config.json`，并把其内容保存到最终 `summary.json` 的 `benchmarkConfiguration` 字段。

- [x] **步骤 5：运行 eMQTT runner 测试**

运行：

```bash
eng/emqtt-bench/run-tests.sh
```

预期：全部通过。

- [x] **步骤 6：提交**

```bash
git add eng/emqtt-bench/run-cell.sh \
  eng/emqtt-bench/run-matrix.sh \
  eng/emqtt-bench/tests \
  eng/emqtt-bench/README.md
git commit -m "bench: expose mqttzero tuning knobs"
```

---

### 任务 5：观测 Egress 积压和待 Flush 编码字节

**文件：**
- 修改：`src/MqttZero.Server/Broker/MqttZeroFanoutDiagnostics.cs`
- 修改：`src/MqttZero.Server/Broker/MqttZeroFanoutDiagnosticsSnapshot.cs`
- 修改：`src/MqttZero.Server/Egress/MqttZeroSessionEgress.cs`
- 修改：`benchmarks/MqttZero.BrokerBenchmarks/Program.cs`
- 测试：`tests/MqttZero.Tests/Broker/FanoutDiagnosticsTests.cs`
- 测试：`tests/MqttZero.Tests/Egress/MqttZeroEgressFlushTests.cs`

**接口：**
- 输入：启用诊断时的 Egress 状态变化
- 输出以下 internal 快照字段：
  - `CurrentEgressFlushPendingCount`
  - `CurrentEncodedBytesAwaitingFlush`
  - `MaxEgressFlushPendingCount`
  - `MaxEncodedBytesAwaitingFlush`
  - `PendingQueueDepth`
  - `InflightQueueDepth`
- 新增 internal 方法：
  - `RecordEgressFlushStarted(long encodedBytes)`
  - `RecordEgressFlushCompleted(long encodedBytes)`

- [x] **步骤 1：先写失败的 Flush 字节统计测试**

在 `FanoutDiagnosticsTests.cs` 中先直接验证计数协议：

```csharp
[Fact]
public void DiagnosticsTrackEncodedBytesUntilFlushCompletes()
{
    MqttZeroFanoutDiagnostics diagnostics = new(enabled: true);
    diagnostics.RecordEgressFlushStarted(64 * 1024);

    MqttZeroFanoutDiagnosticsSnapshot snapshot =
        diagnostics.GetSnapshot();
    Assert.Equal(1, snapshot.CurrentEgressFlushPendingCount);
    Assert.Equal(64 * 1024, snapshot.CurrentEncodedBytesAwaitingFlush);

    diagnostics.RecordEgressFlushCompleted(64 * 1024);

    snapshot = diagnostics.GetSnapshot();
    Assert.Equal(0, snapshot.CurrentEgressFlushPendingCount);
    Assert.Equal(0, snapshot.CurrentEncodedBytesAwaitingFlush);
}
```

- [x] **步骤 2：运行测试并确认失败**

运行：

```powershell
dotnet test --project tests/MqttZero.Tests/MqttZero.Tests.csproj `
  --filter "FullyQualifiedName~DiagnosticsTrackEncodedBytesUntilFlushCompletes"
```

预期：快照字段不存在。

- [x] **步骤 3：增加仅诊断启用时生效的计数**

在 `MqttZeroFanoutDiagnostics` 中增加：

```csharp
internal void RecordEgressFlushStarted(long encodedBytes)
{
    if (!_enabled)
    {
        return;
    }

    Interlocked.Increment(ref _currentEgressFlushPendingCount);
    Interlocked.Add(
        ref _currentEncodedBytesAwaitingFlush,
        encodedBytes);
    InterlockedMax(
        ref _maxEgressFlushPendingCount,
        Interlocked.Read(ref _currentEgressFlushPendingCount));
    InterlockedMax(
        ref _maxEncodedBytesAwaitingFlush,
        Interlocked.Read(ref _currentEncodedBytesAwaitingFlush));
}

internal void RecordEgressFlushCompleted(long encodedBytes)
{
    if (!_enabled)
    {
        return;
    }

    Interlocked.Decrement(ref _currentEgressFlushPendingCount);
    Interlocked.Add(
        ref _currentEncodedBytesAwaitingFlush,
        -encodedBytes);
}
```

在 `MqttZeroSessionEgress` 中：

```text
进入 FlushPending 时调用 RecordEgressFlushStarted(encodedBytes)
Flush 成功、失败或 Stop 释放待处理批次时调用 RecordEgressFlushCompleted(encodedBytes)
```

诊断关闭时不得执行这些更新。每个批次必须携带进入时的精确字节数，保证成功和失败路径扣除一致。

- [x] **步骤 4：增加队列深度**

在入队和出队时维护聚合计数：

```text
PendingQueueDepth = 所有 Egress pending 计数变化之和
InflightQueueDepth = Inflight Map 数量变化之和
```

禁止每 100 ms 扫描全部会话。

- [x] **步骤 5：运行定向测试**

运行：

```powershell
dotnet test --project tests/MqttZero.Tests/MqttZero.Tests.csproj `
  --filter "FullyQualifiedName~FanoutDiagnosticsTests|FullyQualifiedName~MqttZeroEgressFlushTests"
```

预期：通过，包含 Stop、取消和失败路径。

- [x] **步骤 6：运行分配和关闭测试**

运行：

```powershell
dotnet test --project tests/MqttZero.Tests/MqttZero.Tests.csproj `
  --filter "FullyQualifiedName~Allocation|FullyQualifiedName~Shutdown"
```

预期：通过；诊断关闭时热路径仍为零分配。

- [x] **步骤 7：提交**

```bash
git add src/MqttZero.Server/Broker/MqttZeroFanoutDiagnostics.cs \
  src/MqttZero.Server/Broker/MqttZeroFanoutDiagnosticsSnapshot.cs \
  src/MqttZero.Server/Egress/MqttZeroSessionEgress.cs \
  benchmarks/MqttZero.BrokerBenchmarks/Program.cs \
  tests/MqttZero.Tests/Broker/FanoutDiagnosticsTests.cs \
  tests/MqttZero.Tests/Egress/MqttZeroEgressFlushTests.cs
git commit -m "perf: instrument egress backlog and flush bytes"
```

---

### 任务 6：建立 Linux 开发服务器上的 MQTTnet 基线

**文件：**
- 新建：`perf/baseline/linux-dev-mqttnet-baseline-2026-11-28.md`
- 新建：`perf/baseline/linux-dev-mqttnet-baseline-2026-11-28/`
- 修改：`docs/performance/positioning.md`

**接口：**
- 输入：干净的 `master` 和任务 1 至任务 5 的测试基础设施
- 输出：后续所有实验使用的可重复基线

- [ ] **步骤 1：运行代码和测试门禁**

运行：

```powershell
dotnet restore --locked-mode
dotnet build -c Release --no-restore
dotnet test --project tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-build
dotnet test --project tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release --no-build
pwsh ./perf/verify-allocation.ps1
dotnet format --verify-no-changes --no-restore
```

预期：全部退出码为 `0`；不要求 Dashboard。

- [x] **步骤 2：运行目标格快速基线**

运行：

```powershell
./eng/emqtt-bench/run-linux-dev.ps1 `
  -SshConfigPath $env:MQTTZERO_SSH_CONFIG `
  -Quick `
  -Mode throughput `
  -Qos 1 `
  -Subscribers 500 `
  -Payload 65536 `
  -StartPort 19400
```

预期：两个 Broker 投递完整、失败数为零。快速结果只用于趋势判断，不进入最终中位数结论。

- [ ] **步骤 3：运行目标单元格正式确认**

运行：

```powershell
./eng/emqtt-bench/run-linux-dev.ps1 `
  -SshConfigPath $env:MQTTZERO_SSH_CONFIG `
  -Mode throughput `
  -Qos 1 `
  -Subscribers 500 `
  -Payload 65536 `
  -Repetitions 3 `
  -StartPort 19400
```

预期：每个 Broker 三轮有效，交替顺序，使用同一个 `host.json`。

- [ ] **步骤 4：运行限速尾延迟门禁**

运行：

```powershell
./eng/emqtt-bench/run-linux-dev.ps1 `
  -SshConfigPath $env:MQTTZERO_SSH_CONFIG `
  -Mode latency `
  -Qos 0,1 `
  -Subscribers 1,100 `
  -Payload 32,8192 `
  -Repetitions 3 `
  -StartPort 19500
```

预期：所有延迟单元格 QoE 覆盖率至少 `99%`。

- [ ] **步骤 5：编写基线报告**

报告必须包含：

```text
机器信息和 host.json
提交 SHA
完整命令
每轮原始值
吞吐中位数
p50/p95/p99 中位数
Broker 每 1000 次投递 CPU
Broker 峰值 RSS/PSS
客户端 CPU
无效和失败运行数量
已知差距
```

禁止写容量声明。报告标题必须标注“Linux 开发服务器 2 vCPU 开发级证据”。

- [ ] **步骤 6：提交**

```bash
git add perf/baseline/linux-dev-mqttnet-baseline-2026-11-28.md \
  perf/baseline/linux-dev-mqttnet-baseline-2026-11-28 \
  docs/performance/positioning.md
git commit -m "perf: establish linux dev mqttnet baseline"
```

---

### 任务 7：诊断 `QoS 1 / 500 / 64 KiB` 大包扇出

**文件：**
- 新建：`eng/emqtt-bench/run-sweep.sh`
- 新建：`eng/emqtt-bench/tests/test-run-sweep.py`
- 新建：`perf/baseline/linux-dev-qos1-500-64k-diagnostics-2026-11-28.md`

**接口：**
- 输入：`run-linux-dev.ps1`、任务 5 诊断字段、任务 4 调参参数
- 输出：缓冲区预算和 Egress 批次参数的对照结果

- [ ] **步骤 1：定义 sweep 文件格式**

```text
name,max_buffered_bytes,batch_bytes,batch_messages,batch_duration_us
default,536870912,524288,128,500
budget-256m,268435456,524288,128,500
budget-128m,134217728,524288,128,500
largebatch-4,536870912,262144,4,500
largebatch-2,536870912,131072,2,500
```

- [ ] **步骤 2：增加解析测试**

测试名称唯一、数值为正、每一行生成独立输出目录。

运行：

```powershell
py -3 -m unittest discover -s eng/emqtt-bench/tests -p "test-run-sweep.py" -v
```

预期：通过。

- [ ] **步骤 3：运行默认诊断**

仅运行 MQTTZero：

```bash
ENABLE_FANOUT_DIAGNOSTICS=1 \
ENABLE_MQTTZERO_MEMORY_PROBE=1 \
MAX_BUFFERED_BYTES=536870912 \
OUTBOUND_BATCH_BYTES=524288 \
OUTBOUND_BATCH_MESSAGES=128 \
OUTBOUND_BATCH_DURATION_US=500 \
MAX_PENDING=1024 \
./run-cell.sh mqttzero 19600 1 500 65536 100 10 /tmp/q1-500-64k-default
```

预期：`MEMORYPROBE` 包含 buffer rented/cached/committed、对象池、托管堆、工作集、Egress pending 和待 Flush 编码字节。

- [ ] **步骤 4：执行 sweep**

每个变体在 Linux 开发服务器运行三轮。需要跨 Broker 比值时，在同一轮交替运行 MQTTnet；仅做 MQTTZero 归因时，标注为 diagnostic-only。

- [ ] **步骤 5：分类瓶颈**

| 观测结果 | 主要分类 |
|---|---|
| `CurrentEncodedBytesAwaitingFlush` 与 RSS 同步变化 | 每连接输出缓冲保留 |
| 排空后 `BufferCachedBytes >= 25%` committed bytes | 大缓冲缓存保留 |
| Pending/Inflight 字节可解释至少一半 RSS 差距 | 队列/背压策略 |
| 平均数据批次低于 2 条且超过一半批次触发 Flush | 批次碎片和 Flush 开销 |
| Publish 编码时间主导 CPU，复制字节随订阅者增长 | 每连接 Payload 复制 |
| Egress drain 和 scheduler pass 高，但队列深度低 | Actor 调度开销 |

没有观测命中分类规则时，禁止实施修复。

- [ ] **步骤 6：编写诊断报告**

记录原始 JSON、等效复制字节数、每 1000 次投递 CPU、峰值 RSS/PSS、buffer cache/rented/committed、pending 和 flush bytes，以及最终分类。

- [ ] **步骤 7：提交**

```bash
git add eng/emqtt-bench/run-sweep.sh \
  eng/emqtt-bench/tests/test-run-sweep.py \
  perf/baseline/linux-dev-qos1-500-64k-diagnostics-2026-11-28.md
git commit -m "perf: attribute large payload fanout cost"
```

---

### 任务 8：根据证据创建单一优化专项计划

**文件：**
- 新建：`docs/superpowers/plans/2026-11-28-linux-dev-fanout-optimization.md`
- 只修改诊断报告确认的源码文件

**接口：**
- 输入：任务 7 中唯一一个被数据证实的分类
- 输出：一个可独立审查的单瓶颈实施计划

- [ ] **步骤 1：只选择一个分类**

选择证据最强的一个分类。禁止在同一个实验中同时修改缓冲区预算、批次大小和调度结构。

- [ ] **步骤 2：编写专项计划**

专项计划必须包含：

```text
明确假设
精确文件
不变量和所有权约束
先失败的测试
最小实现
一轮快速淘汰门禁
三轮 MQTTnet 对比
回滚条件
```

- [ ] **步骤 3：编码前审查专项计划**

出现以下情况必须拒绝：

```text
同时修改多个瓶颈
依赖一次噪声较大的运行
比较不同主机或不同日期的结果
降低投递完整性或 QoS 语义
删除零分配断言
```

- [ ] **步骤 4：只提交专项计划**

```bash
git add docs/superpowers/plans/2026-11-28-linux-dev-fanout-optimization.md
git commit -m "docs: plan the measured fanout optimization"
```

---

### 任务 9：最终 Linux 开发服务器门禁和性能定位

**文件：**
- 修改：`docs/performance/positioning.md`
- 修改：`perf/baseline/broker-comparison-methodology.md`
- 修改：`eng/emqtt-bench/README.md`
- 新建：`perf/baseline/linux-dev-mqttnet-release-gate.md`

**接口：**
- 输入：完成的专项优化和全部正确性门禁
- 输出：最终证据、已知差距和发布建议

- [ ] **步骤 1：运行正确性和资源门禁**

运行：

```powershell
dotnet restore --locked-mode
dotnet build -c Release --no-restore
dotnet test --project tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-build
dotnet test --project tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release --no-build
pwsh ./perf/verify-allocation.ps1
dotnet format --verify-no-changes --no-restore
```

预期：全部通过，关闭、所有权和分配测试不得跳过。

- [ ] **步骤 2：运行最终吞吐矩阵**

```powershell
./eng/emqtt-bench/run-linux-dev.ps1 `
  -SshConfigPath $env:MQTTZERO_SSH_CONFIG `
  -Mode throughput `
  -Qos 1 `
  -Subscribers 1,100,500 `
  -Payload 32,8192,65536 `
  -Repetitions 3 `
  -StartPort 19700
```

预期：全部运行有效且投递完整。所有单元格都要报告；只有达到或超过 `1.00x` 的单元格才能描述为与 MQTTnet 持平。

- [ ] **步骤 3：运行最终延迟矩阵**

```powershell
./eng/emqtt-bench/run-linux-dev.ps1 `
  -SshConfigPath $env:MQTTZERO_SSH_CONFIG `
  -Mode latency `
  -Qos 0,1 `
  -Subscribers 1,100 `
  -Payload 32,8192 `
  -Repetitions 3 `
  -StartPort 20000
```

预期：QoE 覆盖率至少 `99%`；正式门禁使用 p99。

- [ ] **步骤 4：运行打包和外部消费者验证**

```powershell
pwsh ./eng/verify-packages.ps1
```

预期：核心包成功构建，仓库外消费者成功编译。

- [ ] **步骤 5：更新性能定位**

记录：

```text
实际通过的单元格
实际失败的单元格
吞吐和 p99 比值
CPU 和 RSS 证据
Linux 开发服务器 2 vCPU 限制
开发用本机不得作为正式证据的政策
下一项已批准的瓶颈
```

禁止声称在所有场景全面领先 MQTTnet。

- [ ] **步骤 6：提交**

```bash
git add docs/performance/positioning.md \
  perf/baseline/broker-comparison-methodology.md \
  eng/emqtt-bench/README.md \
  perf/baseline/linux-dev-mqttnet-release-gate.md
git commit -m "perf: record the linux dev mqttnet release gate"
```

---

## 预期推进顺序

| 里程碑 | 必须得到的结果 |
|---|---|
| M1：可信主机 | Linux 开发服务器预检和主机指纹强制执行 |
| M2：可信指标 | 吞吐、p95/p99 和资源字段含义彻底分开 |
| M3：可信基线 | 当前 `master` 有可重复的 Linux 开发服务器基线 |
| M4：瓶颈证明 | 大包高扇出只有一个被指标证实的分类 |
| M5：定向修复 | 只接受一个单瓶颈实施计划 |
| M6：最终门禁 | 正确性测试和三轮 MQTTnet 对比全部完成 |

## 停止条件

出现以下情况时停止当前测试活动并修复测量环境：

```text
主机指纹变化
逻辑 CPU 数变化
内存或文件描述符上限变化
MQTTnet 绝对吞吐变化超过 10%，但没有同轮 MQTTZero 对照
运行出现缺失、重复或乱序投递
订阅端接收数少于 publishedMessages * subscribers
延迟档 QoE 覆盖率低于 99%
另一台机器或另一个基准进程仍在生成负载
```

## 开发用本机使用政策

开发用本机可以用于：

```text
编译
单元和集成测试
分配哨兵
协议调试
一次性诊断实验
远程测试活动准备
```

开发用本机不得用于：

```text
发布级 MQTTnet 排名
正式三轮矩阵
跨地点历史结果比较
容量声明
决定生产批次大小或缓冲区默认值
```

如果本地运行对诊断有帮助，必须记录精确的机器指纹，并标注为 `local-diagnostic-not-comparable`。即使只是非正式比较，也必须在同一台本地机器、同一次测试活动中重新运行 MQTTZero 和 MQTTnet。
