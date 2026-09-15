# 为 CPA 增加单账号并发保护、排队与超时换号

> 文档状态：开发前需求与设计基线，功能尚未实现
> 内容类型：需求、设计约束与实施计划
> 编写日期：2026-09-14
> 目标项目：`router-for-me/CLIProxyAPI`（CPA）自维护分支
> 本地代码基线：CPA `v7.3.2`，提交 `7fa443dc8bf8ca2f1ffd81c2472deb31b097b697`
> 参考实现：Sub2API 提交 `bdb42e22f81fcb633ff0a060961211dd2bcb515b`
> 生产边界：本文不授权修改 CPA 代码、生产配置、OAuth 文件、容器或其他服务

本文定义 CPA 第一版账号级并发保护的完整行为。实现必须在不改变现有路由、重试和流式输出语义的前提下，为 Codex OAuth/file 账号增加进程内并发上限、FIFO 排队、等待超时换号和可观测性。全局功能默认关闭，关闭时必须绕过全部新增准入逻辑。

## 1. 目标与成功标准

CPA 需要限制单个账号同时执行的上游请求数量，并在账号满载时优先使用其他空闲账号。只有所有合格账号都没有空闲槽时，请求才进入账号队列。

实现必须满足以下行为：

1. 未配置新字段时，账号选择、上游请求、响应和性能特征与当前官方 CPA 保持一致
2. 每个账号可以独立配置最大活跃请求数、最大排队数和单次排队时限
3. 同优先级的其他账号有空闲槽时，请求立即换到空闲账号，不在繁忙账号上等待
4. 所有候选账号均满时，请求按现有调度顺序选择一个允许排队的账号进入 FIFO 队列
5. 队列已满或等待超时后，请求可以在独立换号预算内尝试另一个账号
6. 一次客户端请求的累计排队时间不能超过全局总等待预算
7. 本地容量不足不能触发上游重试、账号冷却、健康降级或使用量记录
8. 每个成功取得的槽位必须释放一次且只能释放一次
9. 客户端取消后，排队节点和已取得的槽位必须及时清理
10. HTTP、Server-Sent Events（SSE）和 Responses WebSocket 路径不能泄漏槽位或在输出后换号重放

## 2. 第一版范围

第一版以当前生产使用的 Codex OAuth/file 账号为交付范围，但核心协调器和运行时属性不得写死 Codex Provider。

### 2.1 纳入范围

| 执行模式或入口 | 第一版是否限制 | 槽位粒度 | 说明 |
| --- | --- | --- | --- |
| `Manager.Execute` | 是 | 每次实际账号执行尝试 | 包含经统一 Auth Manager 进入的普通请求、图片和视频请求 |
| `Manager.ExecuteCount` | 是 | 每次实际账号执行尝试 | 令牌计数也会消耗账号上游能力 |
| `Manager.ExecuteStream` | 是 | 每次实际账号执行尝试 | 从认证准备前持有到流消费 goroutine 退出 |
| Responses WebSocket | 是 | 每个活跃的 `response.create` | 空闲下游连接不占槽 |
| 同一账号的不同模型 | 是 | 共享账号总额度 | 第一版按稳定 `Auth.ID` 限制，不按模型拆分 |
| 配置文件和管理 API | 是 | 账号级配置 | 管理面板不属于第一版 |

### 2.2 明确排除

第一版不包含以下能力：

- Home 模式下的本地账号并发限制
- Plugin Executor 自身的并发限制
- 绕过 Auth Manager 直接调用 `ProviderExecutor.HttpRequest` 的请求
- New API 用户级并发、用户级队列或计费修改
- API Key 和第三方兼容渠道的配置界面
- 跨进程或跨服务器的分布式并发协调
- CPA 重启后恢复正在排队的 HTTP 请求
- 对已取得 lease 的执行尝试使用“容量换号”逻辑
- 对已经产生下游输出的流式请求换号重放
- 自动修改 OAuth Token、API Key、套餐或额度

后续版本如果纳入 Plugin Executor、直接 `HttpRequest` 或多实例部署，必须复用同一个准入接口，不能在 Handler 中复制一套计数器。

### 2.3 Home 模式边界

`Manager.HomeEnabled() == true` 时，Home 是唯一并发权威。新的本地协调器必须完全绕过，不读取账号级本地限额，也不能叠加第二层 lease。

管理 API 在 Home 模式下可以返回账号配置，但运行快照必须标记：

```json
{
  "account_concurrency_mode": "home_authoritative",
  "local_limit_effective": false
}
```

切换 Home 模式时，已经由旧模式取得的 lease 自然释放。实现不能强杀正在执行的请求。

## 3. 术语和计数语义

本节固定实现和验收使用的术语，避免把本地容量处理与现有上游重试混为一谈。

| 术语 | 定义 |
| --- | --- |
| active | 已取得账号 lease 且尚未释放的执行尝试数量 |
| waiting | 已成功进入账号 FIFO 队列且尚未取得 lease、取消或超时的请求数量 |
| capacity probe | 对候选账号执行一次非阻塞 `TryAcquire`；仅探测容量，不计为换号 |
| queue target | 所有候选账号均无空闲槽后，本次请求选择进入队列的账号 |
| capacity switch | queue target 队列已满或等待超时后，改选另一个 queue target |
| upstream attempt | 已完成账号准入，随后调用实际 Executor 的一次执行尝试 |
| client retryable | 客户端收到错误后可以重新发送新请求 |
| CPA internal retry | CPA 在同一个客户端请求内依据 `request-retry` 再次执行上游 |

`max-account-switches` 只限制 capacity switch。`max-total-wait` 和 capacity switch 计数都属于整个客户端请求，必须在 `Execute`、`ExecuteCount` 或 `ExecuteStream` 的公开入口创建，并跨现有 `request-retry` 轮次共享：

- 初始候选选择不计数
- 探测多个无空闲槽账号不计数
- queue target 的队列已满并改选其他 queue target 时加一
- 在 queue target 等待超时并改选其他 queue target 时加一
- `max-account-switches = 0` 仍允许探测所有候选账号的空闲槽，但只允许选择第一个 queue target
- 取得 lease 后发生的认证或上游错误不计入 capacity switch，继续遵循现有 CPA 重试规则
- 一个上游尝试结束并进入普通重试后，`capacityExcluded` 为新准入周期清空；已消耗的总等待预算和 capacity switch 次数不重置

## 4. 配置契约

所有来源必须归一化为相同的运行时属性。配置解析必须拒绝非法值，不能静默修正或回退。

### 4.1 全局配置

`config.yaml` 增加：

```yaml
account-concurrency:
  enabled: false
  max-total-wait: "30s"
  max-account-switches: 2
  max-total-waiters: 10000
  store: "memory"
```

| 字段 | 默认值 | 允许范围 | 含义 |
| --- | ---: | ---: | --- |
| `enabled` | `false` | 布尔值 | 关闭时绕过全部本地准入逻辑 |
| `max-total-wait` | `30s` | `100ms` 至 `5m` | 单个客户端请求的累计排队上限 |
| `max-account-switches` | `2` | `0` 至 `100` | queue target 最多切换次数 |
| `max-total-waiters` | `10000` | `1` 至 `100000` | 进程内所有账号的排队总上限 |
| `store` | `memory` | 仅 `memory` | 第一版协调器实现 |

配置规则如下：

- 未识别的 `store` 必须导致启动失败或热更新被拒绝
- 全局配置和账号配置即使在 `enabled = false` 时也必须校验，避免启用时才暴露错误
- 启动配置非法时，CPA 拒绝启动并指出完整字段路径
- 热更新配置非法时，CPA 保留最后一次有效配置并记录结构化错误
- `max-total-wait` 只统计实际排队时间，不包含路由、认证准备、上游执行和响应传输时间
- 请求在第一次准备执行 capacity probe 时固定总等待预算；后续热更新和普通重试不能重置或扩大该预算

### 4.2 OAuth/file 账号配置

认证文件顶层增加以下可选字段：

```json
{
  "max_concurrency": 2,
  "max_waiting": 6,
  "wait_timeout_ms": 8000
}
```

| 字段 | 默认值 | 第一版允许范围 | 含义 |
| --- | ---: | ---: | --- |
| `max_concurrency` | `0` | `0` 至 `10000` | 单账号最大 active；`0` 表示无限制 |
| `max_waiting` | `0` | `0` 至 `10000` | 单账号最大 waiting；`0` 表示不排队 |
| `wait_timeout_ms` | `0` | `0` 或 `100` 至 `300000` | 单次进入该账号队列的等待上限 |

字段组合必须满足以下规则：

- 三个字段只能是 JSON 非负整数，不能接受字符串、浮点数或布尔值
- `max_concurrency = 0` 时，`max_waiting` 和 `wait_timeout_ms` 必须同时为 `0`
- `max_concurrency > 0` 且 `max_waiting = 0` 时，`wait_timeout_ms` 必须为 `0`
- `max_concurrency > 0` 且 `max_waiting > 0` 时，`wait_timeout_ms` 必须至少为 `100`
- `wait_timeout_ms` 可以大于当前 `max-total-wait`；实际等待仍取两者较小值，避免全局配置下调导致所有账号文件失效

文件来源的非法更新不能覆盖运行中的有效 Auth：

- 热更新时保留该账号最后一次有效元数据并记录字段级错误
- 首次启动且没有有效旧值时，不注册该账号，并在管理 API 中展示加载错误
- OAuth 刷新和 Token 更新必须保留这三个顶层元数据字段

### 4.3 运行时归一化

OAuth/file 字段必须归一化到 `Auth.Attributes`：

| 文件字段 | 运行时属性 |
| --- | --- |
| `max_concurrency` | `account_concurrency.max_concurrency` |
| `max_waiting` | `account_concurrency.max_waiting` |
| `wait_timeout_ms` | `account_concurrency.wait_timeout_ms` |

实现必须提供一个集中解析函数，将字符串属性转换为不可变的 `AccountLimit`。Selector、Executor 和 Handler 不能各自解析字段。

### 4.4 管理 API 写入

管理 API 必须把三个账号字段作为一个配置元组校验。单字段 PATCH 可以保留，但校验必须基于“旧值加本次修改”形成的最终元组。

一次成功修改必须从调用方视角原子完成：

1. 校验字段和值
2. 持久化认证文件元数据
3. 同步运行中 `Auth.Metadata` 和 `Auth.Attributes`
4. 通知协调器应用新限额
5. 返回新配置和运行快照

持久化或运行态同步失败时，Handler 必须把文件和内存都恢复到修改前快照；回滚失败时返回 500 并记录不含密钥的高优先级错误。任何一步失败都不能向调用方返回成功。Token、refresh token 和 API Key 不得出现在请求日志、错误消息或修改响应中。

### 4.5 后续 API Key 契约

第一版不要求启用 API Key 配置，但预留字段名称：

```yaml
max-concurrency: 2
max-waiting: 6
wait-timeout: "8s"
```

后续接入时，字段必须放在实际生成单个 `Auth` 的 API Key 条目上，不能只放在 Provider 容器级别。无独立 key 条目的兼容渠道可以使用 Provider 级限额，但必须明确它对应一个共享 `Auth.ID`。

## 5. 候选账号与容量调度

并发准入位于账号候选排序之后、`prepareRequestAuth` 之前。新增逻辑不能通过反复调用现有 Selector 来探测容量。

### 5.1 候选计划

调度器必须为一次准入生成稳定的 `CandidatePlan`：

1. 按 Provider、模型支持、启用状态、认证有效性、额度和冷却状态过滤账号
2. 应用明确的会话绑定策略
3. 按 Priority 分组
4. 在最高可用 Priority 组内应用现有 round-robin、weighted-round-robin 或 fill-first 顺序
5. 排除本次请求已经等待超时或不再可用的账号
6. 返回有序候选列表和本次计划的调度提交信息

容量探测期间不得重复推进 RR 游标或 WRR 权重状态。只有取得 lease 或正式选定 queue target 时，调度状态才能提交一次。

容量不足不能自动降级到更低 Priority 组。只有当前 CPA 路由规则本来就允许选择该组时，它才可以进入 `CandidatePlan`。

`capacityExcluded`、`upstreamAttempted` 和 `requestRetryRoundExclusions` 必须是三个独立集合：

- `capacityExcluded` 记录本次准入中队列已满或等待超时的账号
- `upstreamAttempted` 记录已实际执行过 Executor 的账号
- `requestRetryRoundExclusions` 保留现有跨重试轮次语义

容量不足不能增加 `upstreamAttempted`，也不能消耗 `max-retry-credentials`。

### 5.2 空闲账号优先

对 `CandidatePlan` 中的账号依次执行非阻塞 capacity probe：

1. 无限并发账号立即视为取得准入，不创建协调器状态
2. 有限并发账号调用 `TryAcquire`
3. 成功时提交该候选的调度状态并开始执行
4. 满载时记录为可选 queue target，继续探测后续候选
5. 所有候选都无法立即取得槽位后，按同一稳定顺序选择第一个允许排队的账号

账号已有 waiter 时，新到请求的 `TryAcquire` 必须返回满载。新请求不能绕过 FIFO 队首，即使释放和探测发生在同一时刻。

### 5.3 Queue target 选择

只选择 `max_waiting > 0` 的满载账号作为 queue target。入队前必须同时检查：

- 账号队列未达到 `max_waiting`
- 全局 waiting 未达到 `max-total-waiters`
- 请求剩余总等待预算大于 `0`
- 客户端 Context 尚未取消
- 账号仍然启用且限额仍然有效

入队失败后的处理如下：

- 账号队列满：加入 `capacityExcluded`，消耗一次 capacity switch，再选择下一个 queue target
- 全局队列满：立即结束，不继续扫描其他账号队列
- 账号禁用或删除：加入 `capacityExcluded`，不消耗 capacity switch，重新生成候选计划
- 客户端取消：立即结束且不写 HTTP 响应

### 5.4 等待预算

请求级预算由公开执行入口创建，并跨该客户端请求的全部普通重试轮次传递：

```text
admission_started_at
initial_total_wait_budget
remaining_total_wait_budget
capacity_excluded_auth_ids
capacity_switches
total_wait_duration
```

一次入队的截止时间取以下三者最早值：

```text
min(
  enqueue_time + account_wait_timeout,
  enqueue_time + remaining_total_wait_budget,
  client_context_deadline
)
```

从入队成功到取得 lease、超时或取消的实际时间计入 `total_wait_duration`，并从 `remaining_total_wait_budget` 扣除。capacity probe、队列满检查、认证准备、上游执行和普通重试计算时间不扣减该预算。

### 5.5 准入状态机

```text
PLAN
  -> PROBE               生成稳定候选顺序
  -> REJECT              没有合格账号

PROBE
  -> ACQUIRED            候选立即取得槽位
  -> ENQUEUE             所有候选均满
  -> REJECT              无账号允许排队

ENQUEUE
  -> WAITING             原子入队成功
  -> SWITCH              账号队列已满
  -> REJECT              全局队列满或预算耗尽

WAITING
  -> ACQUIRED            队首取得保留槽位
  -> SWITCH              单账号等待超时
  -> REPLAN              账号禁用或删除
  -> CANCELED            客户端取消
  -> REJECT              总等待预算耗尽

ACQUIRED
  -> EXECUTING           开始认证准备
  -> RELEASED            执行前异常
```

一旦进入 `ACQUIRED`，容量状态机不得再执行 capacity switch。后续认证准备或上游错误由现有 CPA 执行和重试逻辑处理；新的执行尝试必须重新经过账号准入。

## 6. FIFO 协调器

第一版使用进程内协调器。协调器按稳定 `Auth.ID` 保存状态，每个账号拥有独立队列，并维护进程级 waiting 总数。

### 6.1 原子和公平性要求

协调器必须满足以下不变量：

- `active >= 0` 且 `waiting >= 0`
- 有限账号正常状态下 `active <= max_concurrency`
- 降低限额时可以暂时出现 `active > max_concurrency`，但不能发放新 lease
- 队列非空时，新到请求不能绕过队首
- 一个 waiter 只能处于 queued、acquired、canceled、timed_out 或 auth_removed 中的一种终态
- 一个 lease 只能改变一次 active 计数
- 账号 active 和全局 active 必须在同一临界区内更新
- 账号 waiting 和全局 waiting 必须在同一临界区内更新

释放槽位时，协调器直接把可用名额保留给 FIFO 队首，再唤醒该 waiter。不能先减少 active、解锁，再让新请求竞争空闲槽。

### 6.2 建议接口

接口草案用于固定行为，不要求实现逐字照搬：

```go
type AccountLimit struct {
    MaxConcurrency int
    MaxWaiting     int
    WaitTimeout    time.Duration
}

type AdmissionOutcome string

const (
    AdmissionAcquired   AdmissionOutcome = "acquired"
    AdmissionQueueFull  AdmissionOutcome = "queue_full"
    AdmissionTimedOut   AdmissionOutcome = "timed_out"
    AdmissionCanceled   AdmissionOutcome = "canceled"
    AdmissionAuthGone   AdmissionOutcome = "auth_unavailable"
    AdmissionGlobalFull AdmissionOutcome = "global_queue_full"
)
```

```go
type Lease interface {
    AuthID() string
    AcquiredAt() time.Time
    Release(reason ReleaseReason)
}

type Coordinator interface {
    TryAcquire(authID string, limit AccountLimit) (Lease, bool, error)
    WaitAcquire(ctx context.Context, authID string, limit AccountLimit, deadline time.Time) (Lease, AdmissionOutcome, error)
    UpdateGlobal(enabled bool, maxTotalWaiters int) error
    UpdateLimit(authID string, limit AccountLimit) error
    RemoveAuth(authID string)
    Snapshot(authID string) AccountConcurrencySnapshot
    SnapshotAll() []AccountConcurrencySnapshot
    Close() error
}
```

`Lease.Release` 内部必须使用 `sync.Once`。重复调用不得返回错误、重复记录计数或令 active 变成负数。无限并发和关闭全局开关后放行的 waiter 可以取得 no-op lease，以保持调用方生命周期一致。

账号没有 active、waiting、累计计数且已经从 Auth Manager 删除时，协调器应删除对应状态，避免长期运行后按历史 Auth ID 无限增长。

### 6.3 取消和超时

取消、超时和槽位转移必须通过同一个 waiter 状态转换完成：

- Context 取消时，若 waiter 仍在队列中，则原子移除并减少 waiting
- 如果槽位已保留给 waiter，则取消路径负责立即释放该 lease
- timer 到期与释放槽位同时发生时，只能有一个状态转换成功
- 队列移除必须为 O(1)，或者使用可证明不会长期积累的 tombstone 清理机制
- 单元测试不能用 `time.Sleep` 推测时序，必须注入时钟或使用 channel、barrier 和显式时间推进

### 6.4 热更新语义

热更新按以下规则执行：

| 变更 | 已执行请求 | 已排队请求 | 新请求 |
| --- | --- | --- | --- |
| 增加 `max_concurrency` | 不变 | 按 FIFO 补发可用槽位 | 使用新限额 |
| 降低 `max_concurrency` | 不强杀 | 保留队列，等 active 自然回落 | 不发新 lease |
| 降低 `max_waiting` | 不变 | 不驱逐已有 waiter | 新入队使用新上限 |
| 修改 `wait_timeout` | 不变 | 保留入队时固定的截止时间 | 使用新值 |
| 账号改为无限并发 | 不变 | 所有 waiter 按 FIFO 唤醒并放行 | 绕过协调器 |
| 账号禁用或删除 | 自然结束 | 以 `auth_unavailable` 唤醒并重新选号 | 不再进入候选 |
| 全局开关关闭 | lease 自然释放 | 所有 waiter 唤醒并放行 | 立即绕过协调器 |

OAuth Token 刷新不能改变 `Auth.ID` 或清空协调器状态。认证文件被替换且产生新 `Auth.ID` 时，旧账号按删除处理，新账号创建独立状态。

功能开启且有限账号的协调器发生内部错误时必须 fail closed：不调用上游，返回 `concurrency_tracker_unavailable`。功能关闭或账号无限并发时不访问协调器，因此不受协调器故障影响。

## 7. 会话亲和与换号

第一版提供两种容量策略：

```yaml
routing:
  session-affinity-capacity-policy: "strict-wait"
```

| 策略 | 有明确会话绑定 | 无会话绑定 |
| --- | --- | --- |
| `strict-wait` | 只探测和等待绑定账号；超时返回 429 | 正常使用全部候选账号 |
| `wait-then-switch` | 优先绑定账号；等待超时后允许换号并重新绑定 | 正常使用全部候选账号 |

默认值固定为 `strict-wait`。只有两个真实 Codex 账号完成加密内容续接验证后，生产环境才能对相关请求启用 `wait-then-switch`。

`wait-then-switch` 发生换号时，执行层必须显式完成以下动作：

1. 清除本次请求的 pinned auth 限制
2. 把旧账号加入 `capacityExcluded`
3. 重新生成 `CandidatePlan`
4. 新账号取得 lease 后再更新 session affinity 绑定
5. 新账号未取得 lease 时不得提前修改绑定

同一 Responses WebSocket 连接上的连续 `response.create` 分别准入。连接空闲期间不保留账号 lease；关闭连接时仍需调用现有 session 资源清理逻辑。

## 8. 槽位生命周期

lease 在账号和实际执行模型确定后、`prepareRequestAuth` 调用前取得。认证准备、令牌刷新、上游连接和完整响应生命周期都计入 active。

### 8.1 非流式和 Count

以下任一事件发生后释放：

- 认证准备失败
- 请求拦截器在认证后终止执行
- 模型映射后没有可执行模型
- Executor 返回成功或错误
- Context 取消
- panic 被恢复

外层普通重试如果选择同一账号或其他账号，必须取得新的 lease。上一执行尝试的 lease 必须先释放。

### 8.2 SSE

`ExecuteStream` 返回 `StreamResult` 不代表执行结束。lease 必须由包装输出 channel 的 goroutine 持有，并在该 goroutine 退出时释放。

退出条件包括：

- 正常读到上游 channel 关闭
- 收到带错误的 stream chunk
- HTTP 200 后没有有效事件并结束
- 下游停止消费且 Context 取消
- 响应改写或转发失败
- panic 被恢复

下游 Handler 提前返回时，包装层必须取消或排空剩余上游流，直到负责持有 lease 的 goroutine 确认退出。不能只在 Handler 中 `defer lease.Release()`。

### 8.3 Responses WebSocket

每个活跃 `response.create` 独立取得一个 lease。以下情况释放：

- 当前 response 完成
- 上游 WebSocket 或 HTTP fallback 返回错误
- 客户端取消当前 response
- 下游 WebSocket 关闭
- 协议错误导致当前 response 终止

空闲连接、预热事件和仅维护 session 的本地操作不占槽。第一版不支持同一连接内并行执行多个 `response.create`；如果后续允许并行，每个活动 response 必须单独计数。

### 8.4 Exactly-once release

实现必须覆盖所有正常、错误、取消和 panic 路径。release closure 内部使用 `sync.Once`，并记录以下内部信息用于测试和诊断：

- `auth_id`
- `admission_id`
- `acquired_at`
- `released_at`
- `release_reason`

日志不得包含 Token、API Key、提示词或响应正文。

## 9. 与现有重试和错误处理的关系

容量错误发生在实际 Executor 调用之前。它们对客户端可重试，但对 CPA 当前请求的 `request-retry` 不可重试。没有任何健康且支持模型的账号时，必须保留现有 `auth_not_found` 或对应路由错误，不能伪装成本地容量不足。

### 9.1 Typed admission error

实现必须定义可被 `errors.As` 识别的本地准入错误，例如：

```go
type AdmissionError struct {
    Code       string
    HTTPStatus int
    Outcome    AdmissionOutcome
    RetryAfter *time.Duration
}
```

`Execute`、`ExecuteCount` 和 `ExecuteStream` 的外层重试循环必须先识别 `AdmissionError` 并直接返回，不能把本地 429 送入 `shouldRetryAfterErrorWithAttempted`。

本地准入错误不得执行以下动作：

- 调用 `recordExecutionResult` 或等价结果钩子
- 写入账号 `LastError`
- 更新 cooldown 或健康状态
- 增加上游失败数
- 发布成功或失败 usage
- 增加 `upstreamAttempted`
- 消耗 `request-retry` 或 `max-retry-credentials`
- 触发 Antigravity credits fallback

### 9.2 HTTP 错误

| 场景 | HTTP | 内部 code | 客户端可重试 | CPA 内部重试 |
| --- | ---: | --- | --- | --- |
| 所有可选账号不允许排队 | 429 | `credential_queue_full` | 是 | 否 |
| 进程级等待队列已满 | 429 | `credential_queue_global_full` | 是 | 否 |
| 单账号或总等待预算耗尽 | 429 | `credential_queue_timeout` | 是 | 否 |
| 客户端主动断开 | 不写响应 | `context_canceled` | 否 | 否 |
| 协调器关闭或内部异常 | 503 | `concurrency_tracker_unavailable` | 是 | 否 |

响应必须保留 CPA 请求 ID。`Retry-After` 只能在有明确等待建议时返回，并向上取整为 HTTP 支持的秒数；无法估算释放时间时应省略，不能伪造固定值。

错误文案必须明确这是 CPA 本地账号容量不足，不能复用上游 `rate_limit_exceeded`。New API 是否保留内部 code、请求 ID 和 `Retry-After` 必须在灰度前验证。

### 9.3 执行边界

容量换号只发生在尚未取得 lease 的状态。取得 lease 后：

- 认证准备失败走现有认证错误路径
- 实际 Executor 调用前失败走现有请求错误路径
- 上游已开始后走现有普通重试和流式提交边界
- 下游已收到任何响应内容后禁止重放

`upstream_started` 定义为即将调用 `ProviderExecutor.Execute`、`CountTokens` 或 `ExecuteStream` 的时刻。`downstream_committed` 定义为 Handler 已提交最终 HTTP 状态、响应头或第一个响应字节的时刻。

## 10. 可观测性和管理快照

第一版通过结构化日志和管理 API 提供诊断数据。Prometheus 等外部指标系统不属于第一版。

### 10.1 结构化日志

只在状态变化时记录日志：

- `request_id`
- 脱敏 `auth_id`
- `provider`
- `route_model`
- `admission_id`
- `active` 和 `max_concurrency`
- `waiting` 和 `max_waiting`
- `global_waiting` 和 `max_total_waiters`
- `wait_ms` 和 `total_wait_ms`
- `capacity_switches`
- `outcome`
- `release_reason`

`outcome` 使用稳定枚举：`acquired`、`queue_full`、`global_queue_full`、`timed_out`、`canceled`、`auth_unavailable`、`switched`。

普通 capacity probe 满载不逐条写 info 日志，避免高并发日志放大。可以按 debug 级别记录，或者只在最终入队、换号和拒绝时记录。

### 10.2 管理 API 快照

每个账号返回：

```json
{
  "enabled": true,
  "max_concurrency": 2,
  "active": 2,
  "max_waiting": 6,
  "waiting": 3,
  "wait_timeout_ms": 8000,
  "queue_full_total": 4,
  "timeout_total": 2,
  "canceled_total": 1,
  "switch_total": 3,
  "oldest_wait_ms": 950
}
```

管理 API 还必须返回进程级快照：

```json
{
  "mode": "local",
  "enabled": true,
  "active": 8,
  "waiting": 12,
  "max_total_waiters": 10000,
  "global_queue_full_total": 0
}
```

快照是观测数据，不参与计费和请求正确性。计数器使用同步原子更新或协调器临界区更新，不能依赖可能丢事件的异步队列。

### 10.3 隐私和权限

- 管理员可以看到脱敏账号标识和运行快照
- 普通 API 客户端只能收到请求 ID 和通用容量错误
- 日志和快照不得暴露 OAuth Token、API Key、完整认证文件路径、提示词或响应正文
- 配置修改必须沿用现有管理 API 鉴权

## 11. 源码改造落点

以下位置基于 CPA `v7.3.2` 和提交 `7fa443dc8bf8ca2f1ffd81c2472deb31b097b697`。开发前必须再次核对 checkout，不得直接在生产容器内编辑源码。

### 11.1 配置和认证元数据

- `internal/config/config.go`、`internal/config/config_types.go`：增加全局配置和后续静态凭证字段
- `internal/config/parse.go`、`internal/config/config_load.go`：在两个加载入口应用相同默认值和严格校验
- `internal/watcher/diff/config_diff.go`：展示并发配置热更新差异
- `internal/watcher/synthesizer/file.go`：归一化 OAuth/file 元数据
- `internal/watcher/synthesizer/config.go`：为后续静态凭证复用同一属性
- `internal/api/handlers/management/auth_files.go`：返回配置、加载错误和运行快照
- `internal/api/handlers/management/auth_files_fields.go`：原子校验并同步 Metadata、Attributes 和协调器
- `internal/store/objectstore.go`、`internal/store/postgresstore.go`、`internal/store/gitstore.go`：确认任意元数据字段读写不丢失

### 11.2 核心协调器

建议新增 `sdk/cliproxy/auth/concurrency/`：

```text
types.go        限额、结果、错误和快照
manager.go      对外 Coordinator 和生命周期
memory.go       单实例状态与全局计数
queue.go        每账号 FIFO 队列
lease.go        幂等 release
clock.go        可注入时钟
metrics.go      快照和计数
*_test.go       并发、取消、竞态和泄漏测试
```

### 11.3 调度和执行

- `sdk/cliproxy/auth/scheduler.go`：提供稳定候选计划和单次调度提交
- `sdk/cliproxy/auth/conductor_selection.go`：隔离 capacity、upstream attempt 和 retry 排除集合
- `sdk/cliproxy/auth/conductor_execution.go`：在三个执行入口接入准入，并让 AdmissionError 绕过外层重试
- `sdk/cliproxy/auth/conductor_stream.go`：由流消费 goroutine 持有和释放 lease
- `sdk/cliproxy/auth/conductor_cooldown.go`：在现有运行配置快照更新点应用全局开关和热更新
- `sdk/cliproxy/auth/conductor_home.go`：明确 Home 模式绕过本地协调器
- `sdk/api/handlers/handlers_errors.go`：安全输出 429、503 和 `Retry-After`
- `sdk/api/handlers/openai/openai_responses_websocket.go`：按活跃 `response.create` 管理 lease 和 pinned auth

协议 Handler 只负责协议输出。OpenAI、Anthropic、Gemini Handler 中不能复制账号准入逻辑。

## 12. 参考实现

本需求参考 Sub2API 的等待计划、立即占槽、排队人数和限时等待思路，但不复制其 Handler 失败分支：

| Sub2API 设计 | CPA 采用方式 |
| --- | --- |
| `AccountWaitPlan` | CPA 使用 `CandidatePlan` 和请求级等待预算 |
| `TryAcquireAccountSlot` | CPA 对稳定候选执行非阻塞 capacity probe |
| `IncrementAccountWaitCount` | CPA 原子检查账号和进程级 waiting 上限 |
| `AcquireAccountSlotWithWaitTimeout` | CPA 使用 Context、可控时钟和明确截止时间 |
| sticky 与 fallback 的不同等待计划 | CPA 使用 `strict-wait` 和 `wait-then-switch` |
| Redis 计数 | 第一版使用内存接口，多实例版本另行设计 |

固定参考源码：

- [Sub2API gateway service](https://github.com/Wei-Shaw/sub2api/blob/bdb42e22f81fcb633ff0a060961211dd2bcb515b/backend/internal/service/gateway_service.go)
- [Sub2API OpenAI scheduling](https://github.com/Wei-Shaw/sub2api/blob/bdb42e22f81fcb633ff0a060961211dd2bcb515b/backend/internal/service/openai_gateway_scheduling.go)
- [Sub2API gateway helper](https://github.com/Wei-Shaw/sub2api/blob/bdb42e22f81fcb633ff0a060961211dd2bcb515b/backend/internal/handler/gateway_helper.go)
- [Sub2API Responses handler](https://github.com/Wei-Shaw/sub2api/blob/bdb42e22f81fcb633ff0a060961211dd2bcb515b/backend/internal/handler/gateway_handler_responses.go)
- [Sub2API configuration example](https://github.com/Wei-Shaw/sub2api/blob/bdb42e22f81fcb633ff0a060961211dd2bcb515b/deploy/config.example.yaml)

## 13. 分阶段开发计划

每个阶段必须满足完成门槛后才能进入下一阶段。

### 阶段 0：冻结现有行为

交付物：

- 从官方 CPA `v7.3.2` 建立自维护分支和回滚标签
- 固定本文参考的 Sub2API 提交
- 为 RR、WRR、fill-first、priority、session affinity、retry 和 cooldown 建立基线测试
- 记录 Auth Manager、Plugin Executor、Home、SSE 和 Responses WebSocket 的实际入口
- 使用两个测试账号验证 Codex encrypted content 跨账号续接

完成门槛：现有账号选择、重试和流式提交边界已有自动化基线，外部验证结果已记录。

### 阶段 1：配置和管理契约

交付物：

- 全局配置、默认值和严格校验
- OAuth/file 元数据到 `Auth.Attributes` 的统一归一化
- OAuth 刷新和三个存储后端的字段保留测试
- 管理 API 的读取、修改和运行态同步
- `config.example.yaml` 和迁移说明

完成门槛：省略新增配置时行为不变；非法启动配置失败；非法热更新保留最后一次有效值。

### 阶段 2：内存协调器

交付物：

- 非阻塞 `TryAcquire`
- 无插队 FIFO 和进程级 waiting 上限
- 单账号超时、总等待预算和 Context 取消
- 幂等 lease、运行快照和热更新
- 可注入时钟和确定性竞态测试

完成门槛：协调器包通过 `go test -race`；active 不越过有效限额；测试结束后 active、waiting 和 goroutine 均归零。

### 阶段 3：候选计划和普通请求

交付物：

- 不重复推进 RR/WRR 状态的 `CandidatePlan`
- 空闲账号优先和 queue target 选择
- 独立 capacity switch 预算
- `Execute` 和 `ExecuteCount` 的完整 lease 生命周期
- AdmissionError 绕过现有普通重试和 Antigravity fallback

完成门槛：未拥塞时路由比例不偏移；容量不足不产生上游请求、冷却、失败结果或 usage。

### 阶段 4：SSE 和 Responses WebSocket

交付物：

- 流消费 goroutine 持有 lease
- 正常流、错误流、空流和客户端取消释放测试
- 每个 `response.create` 独立准入
- pinned auth 的 `strict-wait` 和 `wait-then-switch` 行为
- 下游提交后禁止重放

完成门槛：所有流和 WebSocket 终止路径无槽位或 goroutine 泄漏。

### 阶段 5：日志和管理快照

交付物：

- 结构化准入日志
- 账号和全局运行快照
- 管理 API 配置元组修改
- 敏感信息泄漏检查

完成门槛：管理员无需读取 Docker 原始日志即可定位排队、超时、取消和换号原因。

### 阶段 6：全量回归和压力测试

至少覆盖：

- 并发 `1`、`2`、`10` 和无限模式
- 50、100、500 个并发请求
- 账号队列满、全局队列满、单账号超时和总预算耗尽
- 释放、取消、超时、热更新同时发生
- 账号禁用、删除、OAuth 刷新和 Auth ID 变化
- Home 模式启停和本地协调器绕过
- 上游 401、429、500、503 和 HTTP 200 空流
- `go test ./...`、`go test -race ./...`、编译验证和关键基准

测试必须使用可控时钟或确定性同步，不得用墙上时钟 `time.Sleep` 判断 FIFO、超时或竞态结果。

Go 代码改动后的最低编译验证为：

```bash
go build -o test-output ./cmd/server
rm test-output
```

完成门槛：无数据竞争、无计数或 goroutine 泄漏；未拥塞流量的路由比例与基线没有统计显著偏移。

### 阶段 7：灰度发布

1. 构建固定版本和 `linux/amd64` 镜像，不使用 `latest`
2. 保持功能关闭，先验证自维护版本与官方版本行为一致
3. 只为一个低风险测试账号启用，例如并发 `2`、排队 `4`、单账号等待 `5s`、总等待 `15s`
4. 观察 active、waiting、超时、换号、首字时间、429、空响应和 encrypted content 错误
5. 验证 New API 对请求 ID、错误 code 和 `Retry-After` 的透传
6. 稳定后逐账号开启，不一次性启用全部账号

回滚优先关闭 `account-concurrency.enabled` 并热更新。若热更新验证失败，只重建 CPA 单个容器并回滚到官方固定 digest；不得重启 Nginx、New API、PostgreSQL、Redis 或其他容器。

## 14. 验收用例

| 编号 | 场景 | 期望结果 |
| --- | --- | --- |
| AC-01 | 不配置任何新字段 | 无限并发；不创建协调器账号状态；路由和重试与基线一致 |
| AC-02 | 功能开启但账号 `max_concurrency = 0` | 该账号绕过协调器 |
| AC-03 | A 并发为 2，同时发起 5 个请求 | A 的 active 不超过 2，其余请求排队、换号或返回容量错误 |
| AC-04 | A 已满，B 有空闲槽 | capacity probe 选择 B，不在 A 排队，不增加 switch |
| AC-05 | A、B 均满，A 可排队 | 请求按稳定候选顺序进入 A 的 FIFO |
| AC-06 | A 已有 waiter，释放一个槽时新请求到达 | 槽位保留给队首，新请求不能插队 |
| AC-07 | A 队列满，B 队列可用 | 消耗一次 switch，进入 B 队列 |
| AC-08 | `max-account-switches = 0`，A 和 B 中 B 空闲 | 仍能通过 capacity probe 立即选择 B |
| AC-09 | `max-account-switches = 0`，第一个 queue target 超时 | 不再选择第二个队列，返回 429 |
| AC-10 | A 等待超时，B 随后释放槽 | 排除 A，取得 B 的 lease；容量逻辑不产生重复上游尝试 |
| AC-11 | 多账号连续等待 | 累计排队不超过固定总等待 deadline 加调度误差 |
| AC-12 | 单账号队列未满但全局队列已满 | 不入队，返回 `credential_queue_global_full` |
| AC-13 | 排队时客户端取消 | waiter 立即移除；不发上游请求；waiting 归零 |
| AC-14 | 取消和槽位释放同时发生 | waiter 只进入一个终态，active 和 waiting 不为负 |
| AC-15 | 非流式成功、失败和 panic | lease 各释放一次 |
| AC-16 | Count 成功和失败 | lease 各释放一次 |
| AC-17 | SSE 正常、空流、错误和客户端断开 | 流 goroutine 退出后释放，无后台换号重放 |
| AC-18 | WebSocket 空闲连接 | 不占账号槽位 |
| AC-19 | WebSocket 执行一个 `response.create` | 活跃期间占一个槽，response 结束后释放 |
| AC-20 | 会话亲和使用 `strict-wait` | 绑定账号满时不跨账号，超时返回 429 |
| AC-21 | 会话亲和使用 `wait-then-switch` | 超时后取得新账号 lease 才重新绑定 |
| AC-22 | Home 模式启用 | 完全绕过本地协调器，不形成双重 lease |
| AC-23 | Plugin Executor 路由 | 第一版不进入本地账号协调器，文档和日志不宣称受保护 |
| AC-24 | 本地队列满或超时 | 不进入 `request-retry`，不写 LastError、cooldown、result 或 usage |
| AC-25 | 上游 429 | 保持现有上游重试和冷却语义，不误判为本地容量错误 |
| AC-26 | WRR 账号均不拥塞 | 长期请求比例符合原权重，capacity probe 不重复推进状态 |
| AC-27 | 并发从 5 降到 2 | 不杀 active；active 回落前不发新 lease |
| AC-28 | 并发从 2 升到 5 | 按 FIFO 唤醒最多三个可执行 waiter |
| AC-29 | `max_waiting` 下调 | 不驱逐已有 waiter，新请求使用新上限 |
| AC-30 | 账号变为无限并发 | 已排队请求放行，新请求绕过协调器 |
| AC-31 | 账号禁用或删除 | waiter 以 auth unavailable 唤醒并重新选号 |
| AC-32 | 功能运行中关闭总开关 | waiter 放行；新请求绕过；旧 lease 自然释放 |
| AC-33 | OAuth 刷新且 Auth ID 不变 | 配置和 active/waiting 状态不丢失 |
| AC-34 | 非法账号字段热更新 | 拒绝更新并保留最后一次有效运行值 |
| AC-35 | CPA 重启 | 原排队连接失败或取消；新进程计数从零开始，不伪造恢复 |
| AC-36 | 没有健康且支持模型的账号 | 返回现有 auth 或路由错误，不返回本地容量错误 |
| AC-37 | 第一次上游失败后进入普通重试并再次排队 | 总等待预算和 switch 次数不重置，capacityExcluded 开启新周期 |
| AC-38 | 有限账号的协调器内部失败 | fail closed，返回 503，不调用上游 |
| AC-39 | 功能关闭或账号无限并发且协调器不可用 | 请求绕过协调器，保持原执行行为 |

## 15. 风险与保护措施

| 风险 | 保护措施 |
| --- | --- |
| 外层重试把本地 429 当成上游 429 | Typed AdmissionError 在三个公开执行入口提前终止 |
| capacity probe 改变 RR/WRR 比例 | 稳定 CandidatePlan，取得 lease 或选定 queue target 时只提交一次 |
| 新请求绕过 FIFO 队首 | 释放时直接向队首保留槽位，队列非空时 TryAcquire 不放行新请求 |
| 取消与释放竞态导致重复 lease | waiter 单一终态和 `sync.Once` release |
| 流返回后 lease 提前释放 | lease 由流消费 goroutine 持有 |
| pinned auth 无法真正换号 | 换号前显式清 pin，取得新 lease 后才重新绑定 |
| Codex encrypted content 跨账号失败 | 默认 `strict-wait`，真实账号验证后才启用换号 |
| 高队列上限耗尽内存 | 账号和进程双重 waiting 上限，不存请求正文 |
| 热更新驱逐或强杀请求 | 固定既有 deadline，降低限额不撤销 lease，不驱逐已有 waiter |
| 本地容量错误污染健康状态 | 禁止调用 result、cooldown、usage 和 Antigravity fallback |
| Home 与本地限额双重计数 | HomeEnabled 时完全绕过本地协调器 |
| 回滚影响其他服务 | 只热关功能或重建 CPA 单容器，保护 OAuth、配置和其他容器 |

## 16. 工作量估算

| 工作 | 预计人日 |
| --- | ---: |
| 基线测试和 Codex 跨账号验证 | 1 至 2 |
| 配置、认证元数据、存储和管理 API | 2 至 3 |
| 内存协调器、FIFO、取消和热更新 | 3 至 4 |
| 候选计划、普通请求和 Count 接入 | 2 至 3 |
| SSE 和 Responses WebSocket | 2 至 4 |
| 日志、快照、回归和灰度准备 | 2 至 3 |
| 合计 | 12 至 19 |

如果第一版只交付 Codex OAuth/file、配置文件和管理 API，不开发面板，预计为 12 至 19 人日。该估算包含稳定候选计划、确定性竞态测试和现有重试边界改造，不能按“增加一个信号量”的工作量评估。

## 17. 已确定决策与外部验证

以下设计决策已经固定，开发阶段不得自行改变：

- 第一版交付 Codex OAuth/file 账号；核心接口保持 Provider 无关
- 账号限额按稳定 `Auth.ID` 跨模型共享
- Home 模式完全绕过本地协调器
- Plugin Executor 和直接 `HttpRequest` 不属于第一版
- 会话容量策略默认 `strict-wait`
- `max-account-switches` 第一版只提供全局值
- 管理面板不属于第一版，先提供配置文件和管理 API
- Responses WebSocket 按活跃 `response.create` 计数，空闲连接不占槽
- 内存协调器是第一版唯一 store

开发前仍需完成三项外部验证：

1. 使用两个 Codex 测试账号验证 encrypted content 能否跨账号续接
2. 验证 New API 是否保留 CPA 的请求 ID、内部错误 code 和 `Retry-After`
3. 确认压测账号、模型、并发规模和最大允许上游成本

外部验证不会阻塞 `strict-wait` 模式的协调器和普通请求开发，但会阻塞生产启用 `wait-then-switch` 以及最终灰度放量。

所有开发和测试必须在独立 CPA 分支完成。生产部署需另行编写包含备份、固定镜像 digest、OAuth 哈希校验、单容器更新、受保护服务启动时间对比和回滚步骤的执行单。
