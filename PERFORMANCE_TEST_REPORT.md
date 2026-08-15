# 性能测试与验证报告

## 1. 测试目的

本文记录 `distcache` 的自动化检查、核心功能验证、三节点混合负载压测及节点故障恢复测试。测试重点是验证缓存命中、Bloom Filter、singleflight、gRPC 远程读取、失效广播、健康检查和节点恢复在并发场景下能否稳定工作。

## 2. 测试环境

### 2.1 服务端

- 操作系统：Windows amd64
- Go：`go1.26.0 windows/amd64`
- CPU：12th Gen Intel Core i7-12700H
- 节点：3 个进程，gRPC 地址为 `localhost:8001`、`localhost:8002`、`localhost:8003`
- HTTP 演示入口：`0.0.0.0:9999`、`0.0.0.0:9998`、`0.0.0.0:9997`
- 缓存组：`scores`
- 数据集：`score-0000` 至 `score-0999`，另保留 `Tom`、`Jack`、`Sam`
- 模拟数据源延迟：`50ms`
- 压测期间关闭 DEBUG 日志，保留 INFO 和 WARN 日志

### 2.2 压测端

- Ubuntu Server 24.04.4 LTS，运行于 VMware
- 8 个虚拟 CPU
- k6：`v2.2.0 linux/amd64`
- 虚拟机地址：`192.168.86.155`
- 通过 VMware 虚拟网络访问 Windows 宿主机 `192.168.86.1`

服务端和压测虚拟机运行在同一台物理电脑上。虚拟机隔离了操作系统和压测进程，避免压测器与服务端直接运行在同一 Windows 调度环境中，但双方仍共享物理 CPU、内存和 VMware 虚拟网络。

## 3. 自动化检查

### 3.1 单元测试

```powershell
go test ./...
```

结果：`core`、`grpcpeer`、`httppeer`、`bloom`、`cache`、`consistenthash`、`lru`、`singleflight` 等包含测试的包均通过。

### 3.2 静态检查

```powershell
go vet ./...
```

结果：无输出，通过。

### 3.3 Race Detector

```powershell
$env:CGO_ENABLED = "1"
$env:CC = "F:\GoTools\winlibs\mingw64\bin\gcc.exe"
go test -race ./...
```

Windows 环境使用 WinLibs GCC 启用 CGO。所有测试通过，未检测到 `DATA RACE`。

## 4. 基础功能验证

### 4.1 HTTP API

```powershell
curl.exe "http://localhost:9999/get?key=Tom"
curl.exe "http://localhost:9999/get?key=NotExist"
curl.exe -X POST "http://localhost:9999/remove?key=Tom"
curl.exe "http://localhost:9999/stats"
```

结果：存在 key 返回正确值，不存在 key 返回 404，主动删除返回 `ok`，`/stats` 正常暴露组和节点指标。

### 4.2 Bloom Filter

对不存在 key 发起 200,000 次请求，全部按预期返回 404，`BloomRejected` 同步增长，未进入模拟数据源。

### 4.3 Singleflight

删除 `Tom` 后以 50 并发请求同一 key。指标显示仅发生 1 次实际加载，另外 49 个请求共享结果，验证并发回源合并有效。

### 4.4 三节点远程读取

从 8001 请求归属于远程节点的 key，观察到 `PeerReads` 增长；并发首次读取由 singleflight 合并，成功后在入口节点形成本地缓存副本。

### 4.5 失效广播

三节点环境删除一个 key 后，入口节点分别向两个 peer 发送失效消息。`BroadcastSent=2`、`BroadcastAcked=2`、`BroadcastDropped=0`，验证 gRPC 双向流广播和 Ack 链路正常。

### 4.6 容量淘汰

使用 `100 bytes` 缓存容量依次写入 20 个 key。写入后 `CapacityRemovals=13`、`Entries=7`、`Bytes=91`；再次读取最早写入的 key 后，miss、回源和容量淘汰各增加 1。结果符合 LRU 容量淘汰预期。

## 5. k6 混合负载模型

k6 使用 `constant-arrival-rate` 开环模型按目标 QPS 生成请求，负载组成如下：

- 约 93%：读取存在的 key；
- 约 4%：读取随机不存在的 key；
- 约 3%：执行 `POST /remove`；
- 存在 key 从 `score-0000` 至 `score-0999` 中按 Zipf 分布选取，模拟少量热点 key 和大量低频 key；
- 常规阶梯测试随机访问三个 HTTP 入口；
- 每档保存 k6 控制台日志和 JSON 汇总文件。

该模型会同时覆盖缓存命中、缓存未命中、Bloom Filter 拒绝、本地回源、远程读取、singleflight 合并、主动删除和失效广播，比固定单 key 压测更接近综合访问场景。

## 6. 混合负载阶梯测试

### 6.1 方法与压测端边界

正式阶梯测试使用同一份二进制和同一组节点，不在档位之间重启服务。Ubuntu 虚拟机中的 k6 使用开环 `constant-arrival-rate`，高档位拆成 2 至 3 个独立分片，避免单个 JavaScript runtime 成为瓶颈。此前用无业务逻辑 HTTP 服务验证过压测端：约 30,000 QPS 时仍能稳定发压，且 P99 约为毫秒级；因此压测端能力能够覆盖本轮服务开始劣化的区间。

Ubuntu `sar` 显示 8k、16k、24k、30k、36k 档的整机平均 idle 分别约为 71.72%、39.77%、26.78%、19.57%、18.34%。高档位压测端 CPU 使用较高，但仍保留约 18% 至 20% idle；同时系统态约为 28% 至 29%，说明 VMware 虚拟网络和协议栈也参与构成本次环境边界。

`dropped_iterations` 表示 k6 未能按计划启动的迭代，不等于已经发给服务端后失败的请求。本轮所有实际发出的请求均返回预期的 200 或 404，`unexpected_response=0`。

### 6.2 阶梯结果

| 目标 QPS | 时长 | 实际完成 QPS | 压测端未发出比例 | P50 | P95 | P99 | 最大延迟 | HTTP/业务异常 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8,000 | 2 分钟 | 7,991.9 | 0.10% | 0.43ms | 45.97ms | 50.77ms | 89.19ms | 0 |
| 16,000 | 2 分钟 | 15,940.4 | 0.37% | 1.31ms | 48.37ms | 52.29ms | 276.99ms | 0 |
| 18,000 | 2 分钟 | 17,988.2 | 0.07% | 2.85ms | 49.97ms | 55.60ms | 401.03ms | 0 |
| 19,000 | 2 分钟 | 18,942.8 | 0.30% | 7.11ms | 57.70ms | 114.46ms | 1.14s | 0 |
| 20,000 | 2 分钟 | 19,801.8 | 1.01% | 7.84ms | 61.29ms | 153.39ms | 1.99s | 0 |
| 24,000 | 3 分钟 | 22,561.4 | 5.99% | 16.79ms | 188.22ms | 319.53ms | 5.36s | 0 |
| 30,000 | 3 分钟 | 23,202.2 | 22.66% | 190.82ms | 579.42ms | 873.36ms | 23.58s | 0 |
| 36,000 | 2 分钟 | 19,059.0 | 33.73% | 168.03ms | 712.91ms | 1,177.47ms | 31.27s | 0 |

8,000 至 18,000 QPS 基本保持线性，18,000 QPS 的 P99 仍约为 56ms。19,000 QPS 时 P99 上升到约 114ms，20,000 QPS 时进一步升至约 153ms，说明明显的延迟拐点位于 **18,000 至 19,000 QPS** 之间。24,000 QPS 后进入明显排队区；目标从 24,000 提高到 30,000 后，实际吞吐只从约 22.6k 增至 23.2k，但 P99 增至约 873ms。36,000 QPS 时吞吐反降，属于明显过载。

结合第 7 节持续验证，本负载和环境下可把 **18,000 QPS** 视为已验证的稳定上限，把 **14,000 至 15,000 QPS** 作为保留约 17% 至 22% 余量的建议长期工作区间。约 **22,000 至 23,000 QPS** 是观测到的混合负载吞吐平台，不是跨硬件、跨部署环境的绝对上限。

### 6.3 业务指标一致性

| 档位 | BroadcastSent 增量 | BroadcastAcked 增量 | BroadcastDropped 增量 | BroadcastFailures 增量 | LoadErrors 增量 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 8,000 | 57,330 | 57,330 | 0 | 0 | 0 |
| 16,000 | 115,802 | 115,802 | 0 | 0 | 0 |
| 24,000 | 244,022 | 244,022 | 0 | 0 | 0 |
| 30,000 | 251,244 | 251,244 | 0 | 0 | 0 |
| 36,000 | 137,420 | 137,420 | 0 | 0 | 0 |

所有档位中广播发送与 Ack 数量相等，没有广播队列丢弃、广播失败或加载错误；三个节点始终保持 `RingNodes=3`、`KnownPeers=2`、`ActiveBroadcastStreams=2`。这说明高压下的主要问题是吞吐和延迟劣化，而不是节点掉线、缓存加载报错或失效广播失效。

### 6.4 pprof 瓶颈分析

在 24,000 QPS 拐点档和 36,000 QPS 过载档，同时采集三个节点各 30 秒 CPU profile。三个节点的结果方向一致：

- `runtime.cgocall` 约占 42% 至 44% 的 flat CPU；
- 调用链主要落在 Windows `syscall.WSASend`、`internal/poll.(*FD).Write` 和网络连接写入；
- `net/http.(*conn).serve` 累计约占 38% 至 40%，HTTP 响应写路径占比较高；
- gRPC `loopyWriter`、`bufWriter.Flush` 等 HTTP/2 写路径累计约占 20%；
- `runtime.semawakeup`、`findRunnable`、`schedule`、`netpoll` 反映高并发网络 I/O 下的 goroutine 唤醒和调度开销；
- LRU、Bloom Filter、一致性哈希和 singleflight 没有进入主要 CPU 热点。

此前 profile 中还发现 gRPC 默认 DNS resolver 频繁执行 DNS/TXT 查询；连接目标改为 `passthrough:///` 后该热点已经消失。本轮剩余瓶颈主要集中在 **Windows 网络系统调用、demo HTTP 入口响应写、节点间 gRPC 写路径和运行时调度**，不是单个缓存算法的锁或计算热点。

### 6.5 优化方向

1. 将三个服务节点部署到 Linux 或独立物理机复测，区分 Windows 网络栈与 VMware 虚拟网络影响。
2. 将客户端流量直接接入更接近实际使用方式的 gRPC/业务入口，单独测量 demo HTTP 层开销。
3. 分别测试纯命中、远程读和删除广播，拆分 HTTP 响应写、gRPC 读取与双向流广播成本。
4. 补充 block、mutex、heap 和 goroutine profile，确认饱和时是否存在锁等待、连接写阻塞或 goroutine 大量堆积。
5. 优化后使用相同负载模型和阶梯复测，以吞吐平台、P95/P99 和 profile 热点变化作为依据。

## 7. 持续稳定性测试

配置：三节点随机入口、Zipf 多 key 混合负载、14,000 QPS、持续 5 分钟。

```text
完成请求：4,200,029
压测端未发出：5（约 0.00012%）
HTTP/业务异常：0
P50：1.371ms
P95：50.312ms
P99：63.073ms
最大延迟：499.008ms
```

约 420 万次实际请求全部成功，延迟没有随运行时间持续恶化。5 次未发出迭代属于极低比例的压测端调度抖动，不影响服务端稳定性结论。

### 7.1 18,000 QPS 十分钟验证

为缩小 16,000 至 24,000 QPS 之间的边界，在 18k、19k、20k 各完成 2 分钟定位测试后，对 18,000 QPS 进行 10 分钟重测。负载仍由两个 k6 分片共同产生，并同步记录虚拟机到宿主机的 ICMP 连通性。

```text
完成请求：10,741,303
实际完成 QPS：17,902.2
目标达成率：99.46%
压测端未发出：58,746（约 0.54%）
HTTP/业务异常：0
P50：约 4.08ms
P95：约 50.71ms
P99：约 59.20ms
最大延迟：6.25s
```

两个分片均无 HTTP 失败或异常响应，P95/P99 与 18k 两分钟测试处于同一数量级，没有随运行时间持续抬升。ping 监控收到 592/594 个响应，没有出现连续网络中断。少量未发出迭代和最大延迟尖峰说明该档位已经接近环境边界，因此将 18k 定义为稳定上限，而不是建议长期满载值。

第一次 18k 十分钟测试在约第 7 分钟发生 VMware 虚拟网络中断，两个分片同时对三个服务端口报告 `network is unreachable`。服务端同期保持三节点和广播流正常，该轮数据不用于服务端性能结论；失败日志与重测日志均保留，便于复盘测试环境故障。

## 8. 节点故障恢复测试

### 8.1 方法

- k6 只请求始终存活的 8001 HTTP 入口，避免客户端直接访问下线端口；
- 负载为 10,000 QPS 混合请求，持续 3 分钟；
- 运行约 30 秒后优雅关闭 8003；
- 等待健康检查摘除节点后重新启动 8003；
- 观察请求结果、哈希环、健康检查和广播流恢复。

### 8.2 k6 结果

```text
完成请求：1,800,000
dropped_iterations：0
HTTP/业务异常：0
P50：0.482ms
P95：46.648ms
P99：50.935ms
最大延迟：249.487ms
```

### 8.3 节点与业务指标

```text
RingNodes：3
KnownPeers：2
ActiveBroadcastStreams：2
PeerEjections：1
PeerRecoveries：1
BroadcastSent：93027
BroadcastAcked：93027
BroadcastUnacked：0
BroadcastDropped：0
PeerReads：20451
LocalLoads：16698
LoadErrors：0
SingleflightShared：562371
```

节点成功完成摘除与恢复，哈希环和两条广播流均恢复。关闭与重启期间入口请求零失败，广播发送与确认数量一致且无丢弃，跨节点读取和本地回源正常。

## 9. Prometheus 与 Grafana

三个节点同时暴露 `/stats` 和 `/metrics`。Prometheus 定时抓取各节点，Grafana 展示缓存命中率、命中/未命中速率、远程读取、本地回源、哈希环节点数、广播确认率、健康检查、节点摘除和恢复等数据。

压测期间验证了：

- 三个 target 均可被抓取；
- Counter 随业务行为单调增长；
- 删除请求、广播发送和 Ack 数量关系符合三节点拓扑；
- 节点下线和恢复能实时反映在 `RingNodes`、`KnownPeers`、`PeerEjections`、`PeerRecoveries` 和 `ActiveBroadcastStreams` 中。

## 10. 结论

- 在三节点、1,000 个 Zipf 随机 key、约 93% 正常读取、4% 不存在 key、3% 主动删除的混合负载下，系统已验证可在 **18,000 QPS 持续运行 10 分钟**；实际完成约 `17.9k QPS`，目标达成率 `99.46%`，P95 约 `50.71ms`，P99 约 `59.20ms`，无 HTTP 或业务异常。
- 18,000 至 19,000 QPS 之间开始出现明显延迟拐点；当前混合负载的观测吞吐平台约为 **22k 至 23k QPS**，已验证稳定上限为 **18k QPS**，建议长期工作区间为 **14k 至 15k QPS**。
- pprof 表明当前主要开销集中在 Windows 网络系统调用、HTTP 响应写、gRPC HTTP/2 写路径和 Go runtime 调度；核心缓存算法没有成为主要 CPU 热点。该结论适用于本次单机 Windows + VMware 网络环境，不作为跨环境绝对上限。
- 节点故障恢复测试中，单一存活入口在 10,000 QPS 下完成 180 万次请求且零失败；故障节点能够被摘除并在重启后恢复，广播链路无消息丢弃。
- Bloom Filter、singleflight、LRU 容量淘汰、gRPC 远程读取、双向流失效广播、健康检查和指标观测均得到验证；全量 Race Detector 未发现数据竞争。

本报告不宣称纯读缓存或其他硬件环境中的绝对 QPS 上限。报告中的稳定点、拐点和吞吐平台只对应上述三节点混合负载与测试环境。

## 11. 测试边界

- 服务端与压测虚拟机共享一台物理电脑，高 QPS 结果会受到 Windows 网络栈、VMware 虚拟网络和宿主机调度影响；
- 压测入口是 demo HTTP API，节点间通信使用 gRPC，结果包含 HTTP 和系统网络栈开销；
- 数据源为带固定 `50ms` 延迟的内存模拟实现，不代表真实数据库；
- 当前未覆盖多物理机部署、网络分区、强制进程崩溃、双节点同时故障和小时级浸泡测试；
- 当前一致性模型为失效广播加 TTL 兜底的最终一致性，不宣称强一致性。
