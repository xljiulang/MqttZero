# 热路径质量审计（2026-11-27）

## 范围与方法

对象是 `src` 中全部 ≥70 行的热路径方法（23 个）：11 个超过 100 行，12 个在 70–100 行之间。
按九条清单逐个过：每消息分配、无 `await` 间隔内的重复求值、跨 `await` 的状态重读、字典双查、
异常与回滚、错误码与关闭顺序、不变量文档、命名、死分支。

原则：只改能指出具体代价的问题；「看起来重复但语义上必须重读」的地方加注释，不改代码。

## 已修复

| 问题 | 位置 | 处置 |
|---|---|---|
| 单订阅标识符每消息分配 `uint[]` | `EnqueuePacketAsync` / `EnqueueLeaseAsync` 的 `uint?` 重载，以及 `MqttZeroQueuedMessage.Rent` | 改为 `ArrayPool<uint>` 租用单元素数组，并强制队列消息复制到自己的池化缓冲区 |
| 离线会话每次新建 `MqttZeroClientConnectionInfo` | `CreateOfflineClientInfo` 的三处调用 | 会话状态缓存 `OfflineClientInfo`，首次访问创建 |
| QoS 2 入站重复查找 | `AcceptInboundQos2Async` 的 `ContainsKey` + 索引器 | 会话消息映射新增 `TryAdd`，去重与插入共用一次哈希查找 |

## 确认必要、明确不改

| 位置 | 现象 | 结论 |
|---|---|---|
| `EnqueueLeaseAsync` | `session.Connection` 在 375 / 378 / 424 / 500 / 560 行附近多次读取 | 每次读取之间都有 `await`，连接可能在等待期间被接管或断开；缓存会读到过期连接。保留重读并加注释 |
| `Mqtt5PropertyReader.TryReadBinary` | `Slice(...).ToArray()` 复制二进制属性值 | 入站缓冲区在报文处理结束后会被管道回收，值必须复制才能存活；池化需要给属性对象加释放跟踪，代价大于收益 |
| `MqttZeroSessionEgress` 状态机 | 大量 `Volatile.Read` | 这些读是与出站工作者共享的状态，去掉会破坏可见性 |
| `EnqueueLeaseAsync` | 注册拦截器时 `new MqttZeroEnqueueContext` | 只在注册了拦截器时发生，零扩展热路径不经过 |
| `RoutePublishLeaseCoreAsync` 等 | `new MqttZeroPreparedPublish(...)` | 该类型是 `readonly struct`，不产生托管分配 |
| `TryReadConnAck` / `TryReadMessageProperties` / `TryReadConnect` | 每个属性一个 `hasXxx` 标志 | 用于拒绝重复属性，是协议要求，不能合并 |

## 逐方法结论（70–100 行）

| 方法 | 结论 |
|---|---|
| `MqttZeroSessionEgress.ExecuteAsync` | 无每消息分配；`Volatile.Read` 为共享状态所需 |
| `MqttPublishPacketCodec.TryReadProperties` | 无重复查找；属性长度与跳过逻辑与 `Mqtt5PropertyLayout` 共用 |
| `DeliverLeaseFastAsync` | 无每消息分配；拦截器上下文按需创建 |
| `SelectBatch` | 只操作会话队列与预算结构，无分配 |
| `DispatchQosOnePublishAsync` | 租约所有权在 `finally` 中释放，无遗漏 |
| `PreparePublishForDispatchAsync` | 返回 `readonly struct`，无分配 |
| `WriteConnAckProperties` | 写入路径使用 `IBufferWriter`，无中间数组 |
| `TryDecodeConnect` | 无重复查找；校验顺序与协议一致 |
| `ProcessInboundPubRelAsync` | 两次 `TryGetValue` 查的是两个不同集合，均为必要 |
| `GetConnAckPropertiesLength` | 纯长度计算，无分配 |
| `MqttPacketCodec.TryDecode` | 首字节分派，无分配 |
| `TryCreateMessageLease` | 失败路径返回原因码，不构造租约 |

## 不在本轮范围

- 不按行数拆分热路径方法；可读性抽取只在能同时消除重复时进行。
- 不修改公开 API、MQTT 语义、`await` 边界与错误码顺序。
- `tests` / `benchmarks` 中的长方法不处理。
