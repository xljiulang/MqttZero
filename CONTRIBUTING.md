# Contributing

## C# 规范

- 所有 public 类型和 public 方法必须有完整 XML 注释。
- 每个文件只能有一个顶层类型，私有嵌套类型除外。
- 所有代码块必须使用大括号，禁止省略方法体的表达式体方法。
- 非静态、非常量字段使用 `_camelCase`；`const` 和 `static readonly` 使用 PascalCase。
- `.cs` 文件使用 CRLF。
- 大数组通过 `ArrayPool<T>` 或 `MemoryPool<T>` 获取并释放。
- 只读数组和列表参数优先使用 Span/Memory。
- 禁止无意义的 `Span<T>.ToArray()`。
- Logger、正则和 JSON 使用源生成。
- 大型领域代码按 `Protocol`、`Broker`、`Sessions`、`Routing`、`Memory`、`Interceptors` 分目录；少量入口类型可保留在项目根目录。
- 文件 namespace 必须与项目根 namespace 加相对目录一致，架构测试和 IDE0130 会校验。

## 开发流程

1. 先写失败测试并运行确认失败。
2. 实现最小改动使测试通过。
3. 运行相关测试和完整解决方案构建。
4. 运行 `SourceConventionTests`。
5. 更新实现计划或 README 中受影响的行为说明；移动或新增文档后运行
   `pwsh -NoProfile -File ./eng/verify-doc-links.ps1`（可选校验，缺链接即返回非零）。
6. 热路径改动必须运行 `perf/verify-allocation.ps1`；性能结论必须附同机 Release 基准结果。

```powershell
dotnet build MqttZero.slnx -c Release
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release
dotnet test tests/MqttZero.Dashboard.Tests/MqttZero.Dashboard.Tests.csproj -c Release
./eng/verify-coverage.ps1
./perf/verify-allocation.ps1
./eng/verify-doc-links.ps1
```

覆盖率门禁按生产程序集执行：

- `MqttZero`：行 86%，分支 70%
- `MqttZero.Server`：行 80%，分支 72%
- `MqttZero.AspNetCore`：行 90%，分支 85%
- `MqttZero.AspNetCore.Dashboard`：行 80%，分支 70%

## 性能规则

- 单线程确定性分配哨兵使用 `GC.GetAllocatedBytesForCurrentThread`，预热后要求 `0 B/op`。
- 并发压力测试验证正确性、Lease 泄漏和重复释放，不以并发线程噪声作为硬分配指标。
- 不得通过替换有利负载、关闭 MemoryDiagnoser 或修改基准参数来掩盖回归。
- 与 MQTTnet 比较时固定 `5.2.0.1603`、Release 构建、相同 Payload 和订阅者数量，并同时报告分配与吞吐。
