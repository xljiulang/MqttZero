# MQTTZero Linux 开发服务器大包扇出优化实施计划

> **致执行代理：** REQUIRED SUB-SKILL：使用 `superpowers:test-driven-development`，按任务执行并保持小步提交。

**目标：** 对 `payload >= 32 KiB` 的普通 PUBLISH 使用更小的内部发送批次，将 64 KiB、500 扇出的输出内存降到接近 MQTTnet，同时保持吞吐不低于 MQTTnet。

**架构：** 公开批次数值保持不变。Egress 在首条待发消息编码长度达到 32 KiB 时，内部将批次上限收紧为 `128 KiB / 2 条`。64 KiB 报文加上主题和属性开销后通常每条单独 Flush；小包继续使用现有配置上限。

**技术栈：** .NET 10、Kestrel PipeWriter、MQTT 5、eMQTT-Bench 0.6.4

**规格来源：** `perf/baseline/linux-dev-qos1-64k-diagnostics-2026-11-28.md`

## 全局约束

- 公开 API、配置属性和默认值不改变。
- 仅对首条消息 `EncodedLength >= 32 * 1024` 的批次应用内部上限。
- 调用方配置小于 `128 KiB / 2 条` 时继续尊重更小值。
- 控制包优先、单连接串行写和完整 MQTT 包边界不变。
- QoS1/2 Inflight 登记、Flush 失败可重发和 Lease 所有权不变。
- 快速门禁使用 `-Quick` 单轮；正式门禁仍需三轮。

---

### 任务 1：建立批次策略

**文件：**
- 新建：`src/MqttZero.Server/Egress/MqttZeroEgressBatchPolicy.cs`
- 新建：`tests/MqttZero.Tests/Egress/MqttZeroEgressBatchPolicyTests.cs`

**接口：**
- 输入：`int encodedLength, int configuredBytes, int configuredMessages`
- 输出：`static (int Bytes, int Messages) Resolve(...)`

- [x] **步骤 1：写失败测试**

```csharp
[Theory]
[InlineData(32 * 1024, 128 * 1024, 2)]
[InlineData(64 * 1024, 128 * 1024, 2)]
[InlineData(8 * 1024, 512 * 1024, 128)]
public void ResolvesAdaptiveLargeMessageLimits(
    int encodedLength,
    int expectedBytes,
    int expectedMessages)
{
    (int bytes, int messages) =
        MqttZeroEgressBatchPolicy.Resolve(
            encodedLength,
            configuredBytes: 512 * 1024,
            configuredMessages: 128);

    Assert.Equal(expectedBytes, bytes);
    Assert.Equal(expectedMessages, messages);
}
```

- [x] **步骤 2：运行并确认失败**

```powershell
dotnet test --project tests/MqttZero.Tests/MqttZero.Tests.csproj `
  -c Release `
  --filter "FullyQualifiedName~MqttZeroEgressBatchPolicyTests"
```

- [x] **步骤 3：实现策略**

```csharp
internal static (int Bytes, int Messages) Resolve(
    int encodedLength,
    int configuredBytes,
    int configuredMessages)
{
    if (encodedLength < 32 * 1024)
    {
        return (configuredBytes, configuredMessages);
    }

    return (
        Math.Min(configuredBytes, 128 * 1024),
        Math.Min(configuredMessages, 2));
}
```

- [x] **步骤 4：验证调用方更小配置被保留**

增加测试，确认 `64 KiB configured` 和 `1 configured message` 结果仍为
`64 KiB / 1`。

- [x] **步骤 5：运行策略测试**

```powershell
dotnet test --project tests/MqttZero.Tests/MqttZero.Tests.csproj `
  -c Release `
  --filter "FullyQualifiedName~MqttZeroEgressBatchPolicyTests"
```

- [x] **步骤 6：提交**

```bash
git add src/MqttZero.Server/Egress/MqttZeroEgressBatchPolicy.cs \
  tests/MqttZero.Tests/Egress/MqttZeroEgressBatchPolicyTests.cs
git commit -m "perf: add adaptive large-message batch policy"
```

---

### 任务 2：接入 Session Egress

**文件：**
- 修改：`src/MqttZero.Server/Egress/MqttZeroSessionEgress.cs`
- 修改：`tests/MqttZero.Tests/Broker/OutboundDeliveryTests.cs`

**接口：**
- 消费：`MqttZeroEgressBatchPolicy.Resolve`
- 产出：首条消息决定本轮预算；循环内继续受同一预算约束

- [x] **步骤 1：写失败集成测试**

增加 QoS0 64 KiB、5 条消息、订阅端阻塞 Flush 的测试。由于编码后的单条报文
超过 `64 KiB`，默认 `128 KiB` 字节预算下预期 `5` 次 Flush。

- [x] **步骤 2：运行并确认失败**

```powershell
dotnet test --project tests/MqttZero.Tests/MqttZero.Tests.csproj `
  -c Release `
  --filter "FullyQualifiedName~LargeMessageBacklogUsesAdaptiveByteBudget"
```

- [x] **步骤 3：在创建预算前解析首条消息**

```csharp
int maximumBytes = services.Options.OutboundBatchBytes;
int maximumMessages = services.Options.OutboundBatchMessages;
if (session.PendingMessages.Count > 0)
{
    int encodedLength = session.PendingMessages[0].EncodedLength;
    (maximumBytes, maximumMessages) =
        MqttZeroEgressBatchPolicy.Resolve(
            encodedLength,
            maximumBytes,
            maximumMessages);
}
```

- [x] **步骤 4：构造预算**

```csharp
MqttZeroEgressBatchBudget budget = new(
    maximumBytes,
    maximumMessages,
    batchStartedTimestamp + batchDurationTicks);
```

- [x] **步骤 5：运行相关测试**

```powershell
dotnet test --project tests/MqttZero.Tests/MqttZero.Tests.csproj `
  -c Release `
  --filter "FullyQualifiedName~OutboundDeliveryTests|FullyQualifiedName~MqttZeroEgress"
```

- [x] **步骤 6：运行分配与关闭回归**

```powershell
pwsh ./perf/verify-allocation.ps1
```

- [x] **步骤 7：提交**

```bash
git add src/MqttZero.Server/Egress/MqttZeroSessionEgress.cs \
  tests/MqttZero.Tests/Broker/OutboundDeliveryTests.cs
git commit -m "perf: cap large-message egress batches"
```

---

### 任务 3：快速性能门禁

**文件：**
- 更新：`perf/baseline/linux-dev-qos1-64k-diagnostics-2026-11-28.md`

- [x] **步骤 1：运行目标格**

```powershell
$env:MQTTZERO_SSH_CONFIG = 'D:/ssh/tencent/linux-dev.json'
$env:ENABLE_MQTTZERO_MEMORY_PROBE = '1'
pwsh -NoProfile -File eng/emqtt-bench/run-linux-dev.ps1 `
  -Quick `
  -Mode throughput `
  -Qos 1 `
  -Subscribers 500 `
  -Payload 65536 `
  -StartPort 19460 `
  -Campaign adaptive-batch-q1-500-64k
```

- [ ] **步骤 2：接受条件**

```text
投递完整且失败为 0
三轮吞吐中位数 >= MQTTnet * 0.98
三轮 RSS 中位数 <= 250 MB
三轮累计分配中位数 <= 1.0 GB
Gen2 仅记录；MEMORYPROBE 会引入额外 GC 噪声
```

- [x] **步骤 3：未达条件时回滚**

若吞吐低于 `0.98x`、RSS 高于 `250 MB` 或累计分配高于 `1 GB`，删除生产实现，
保留实验文档。

---

### 任务 4：正式确认

**文件：**
- 新建：`perf/baseline/linux-dev-adaptive-batch-final-2026-11-28.md`

- [x] **步骤 1：运行三轮目标格**

```powershell
$env:MQTTZERO_SSH_CONFIG = 'D:/ssh/tencent/linux-dev.json'
pwsh -NoProfile -File eng/emqtt-bench/run-linux-dev.ps1 `
  -Mode throughput `
  -Qos 1 `
  -Subscribers 500 `
  -Payload 65536 `
  -Repetitions 3 `
  -StartPort 19470 `
  -Campaign adaptive-batch-final-q1-500-64k
```

- [x] **步骤 2：运行保护格**

```text
QoS1 / 100 / 8 KiB
QoS1 / 1 / 64 KiB
```

- [ ] **步骤 3：门禁**

```text
目标格吞吐 >= MQTTnet
目标格 p99 <= MQTTnet
保护格吞吐回退 <= 5%
保护格 p99 回退 <= 5%
分配哨兵与完整核心测试通过
```

- [ ] **步骤 4：提交证据**

```bash
git add perf/baseline/linux-dev-adaptive-batch-final-2026-11-28.md
git commit -m "perf: validate adaptive large-message batching"
```
