# 单节点历史快照门禁（协议 1）

本接口仅提供来源冻结、原生消息导出和未发布取消释放，不导入目标、不转移好友、不触发普通发送。API/后台负责 SQL ledger、原生 webhook 交叉核验、类型/附件授权、快照发布及客户端协议。首版仅支持严格单节点；本说明不构成部署授权。

## 请求入口

四个接口挂在既有管理 token 中间件后，并额外要求非空管理 token 精确匹配。真实 TCP 对端必须是 loopback；不信任 X-Forwarded-For。无需更改现有配置；没有配置管理 token 时此功能拒绝服务，普通接口行为不变。

- `POST /history/capabilities`：成功为 `200 {"single_node":true,"protocol":1}`；多节点、槽未就绪或能力缺失为 501。API 应在建立来源 reservation 前调用，freeze 时仍会再次检查。
- `POST /history/fence`：请求为 `task_id`（64 位小写 hex）、`source_uid`、`peer_uid`。拒绝相同 UID、包含 `@`、CRC32 碰撞或非原生 canonical 身份。返回同身份和 `source_channel`、`high_watermark`。
- `POST /history/export`：同身份加 `after_seq`、`limit`（1–200）。返回 `source_channel`、`messages`、`next_seq`、JSON bool `more`、`high_watermark`。范围固定为 `(after_seq, high_watermark]`。消息中的 `channel_id` 为原始 canonical，而非普通展示的 logical UID；保留 ID、seq、header、setting、expire、时间戳及正文。非空 StreamNo 或 stream setting 返回 422，由 API 拒绝整项并取消，不静默降级。
- `POST /history/fence/release`：同身份，仅用于未发布取消。存在门禁时 task 和双方身份必须精确匹配；无门禁也耐久保存该任务的取消标记，防止迟到的 freeze 或槽日志重新建立门禁。重复取消幂等，其他任务的门禁不能被释放。API 必须保留 cancelling，直到所有释放确认成功；发布后禁止调用 release。

导出不修改原消息或任何目标频道，也不调用普通发送、分发、webhook 或消息索引写入。消息以原生 MessageResp 为基础，但显式保留存储频道。额外的业务类型过滤在 API 完整核验所有页之后执行：首版只允许文本、图片、语音、视频、文档；任何不支持类型必须整项拒绝，不删除来源。

## 分页

专用导出读取会从原始列补齐 header，避免普通范围迭代器首条记录的兼容行为影响核验；不修改普通读取或原始数据。

全页 JSON 最大 4MiB（4 * 1024 * 1024 字节），包括 base64 正文和响应字段。触及预算的记录不返回、不推进游标，下一页仍可读到。单条超过预算明确报错。`more=true` 时 next_seq 必须前进且等于最后实际返回记录的 seq；到尾时 next_seq 固定为 high_watermark，空尾同样如此。超过水位或 uint32 序号上限的游标拒绝，避免加一溢出及无限分页。

## 耐久性与关闭路径

- Store 按原生频道和类型使用引用计数读写信号量，覆盖 AppendMessages 提案、单频道读取、NoPersist 分发准入、freeze 和 release。带 context 的 append、freeze、export、release 及 freeze 配置锁等待可因请求取消退出。私聊命令频道映射到对应原生来源锁；无关私聊和群聊不共享长时锁，也不使用可能碰撞的哈希分桶。引用表的全局互斥只保护增减引用和建删键，等待者也计入引用，最后释放后立即删除键，不跨任何 Drain、提案或存储 IO。
- freeze 只独占来源频道，等待该来源已进入 Store 的 append 退出，再通过同一 Raft Propose FIFO 的只读屏障等待既有日志实际应用；这包括请求已超时返回、但仍在队列或持久化中的提案。排空失败不创建门禁。最后在 wkdb 按原生私聊频道加锁取高水位并同步写入门禁；慢批次提交仅持有所属频道锁，不阻塞其他频道的冻结或群消息追加。
- 频道配置保存使用独立的按频道互斥锁与该来源的最终取样串行，避免首次 append 创建配置时重入读锁造成 freeze 死锁，也不阻塞无关频道配置；排空过程中不持有配置锁，允许既有频道唤醒完成。
- 门禁使用 wkdb 独立命名空间 0x1c01，并提交独立槽元数据命令。槽提案失败也保留本地门禁，重复同任务取相同水位；其他任务不能覆盖。每个已释放 task 保留耐久取消标记，旧槽日志重放不能复活旧门禁或释放新任务。
- 普通 Store 消息读取、ClientMsgNo、全局搜索、批量读取及消息事件入口对已冻结来源失败关闭；原生 DB 数据保留给导出及 Raft 内部读取。搜索/多频道批量读取不持长时锁跨原始 IO，读取完成后逐频道复核门禁；复核不等待正在独占冻结的来源，而是隐藏它，避免来源的慢 Drain 拖住无关频道结果。按 ID 搜索也检查实际返回频道。全局搜索会跳过冻结来源继续取正常频道结果，不将冻结记录造成的空页当结束。
- 底层普通 AppendMessages 拒绝被冻结来源。Raft 专用重放只允许水位内日志；已有记录必须逐字节一致，并保留原高水位，不能借重放改正文或追加水位外记录。截断不得低于冻结水位。
- 来源门禁不匹配接收目标或其他好友；原生命令频道也检查对应来源，NoPersist 不得绕过。没有门禁的正常聊天沿用原流程。

## 单节点和拓扑边界

创建及导出检查实际 `Cluster.Nodes()` 严格只有本机、所有槽已由本机领导。已存在来源频道必须由本机领导。仅在频道配置确实缺失、无已有 Raft 待写入，并在 wkdb 写锁内确认原始高水位为零且原始消息键也为空时，允许不创建普通频道的空来源分支；耐久水位为零，导出空末页 next_seq=0。配置存在但 leader 未就绪或历史/水位不一致时仍拒绝。

存在门禁时，不允许再保存/迁移该来源的频道 leader 配置；门禁随同槽日志应用到同版本节点后，Store 和底层写入仍拒绝该来源，普通读取隐藏，导出因多节点条件拒绝。无关频道不因此被封禁。此处验证范围是独立 Pebble 间模拟槽命令应用，不是实际多节点切主、扩缩容或网络分区验收。

不支持混跑不认识新槽命令的旧 IM 二进制，不支持复制一份不完整数据目录后直接接管来源。新增节点必须使用同版本并完成槽日志恢复；已有永久 fence 的部署不得直接扩容或强制重配来源 leader。任何多节点正式支持须另行实现和验收。

## 本地测试范围

专项测试使用真实临时 Pebble、fake channel/slot proposer 和 HTTP recorder；另外使用本地单节点频道引擎与丢弃式 transport 验证取消返回后仍在持久化的提案屏障，不使用业务数据库、真实账号或服务器。覆盖并发 append/NoPersist 与 freeze、来源 Drain/槽提案阻塞时无关频道读写和配置仍可完成、命令频道同锁、锁表等待者引用与空闲键回收、多频道 IO 后复核门禁、排队及持久化中的超时提案、配置锁重入、重开 DB、零消息来源、来源读隐藏、固定水位分页、JSON 预算、canonical 身份、stream 拒绝、拓扑、loopback、冲突释放、槽日志重放及普通路径兼容。实际命令和结果以交付记录为准，不将源码检查或 fake proposer 结果称为生产/实机验证。

## 本次 IM 文件范围

以下路径均相对 IM 仓库，不包含 API 仓库或配置文件：

- HTTP：`internal/api/history.go`、`internal/api/history_test.go`、`internal/api/server_http.go`。
- 发送末端：`internal/channel/handler/event_persist.go`、`internal/channel/handler/history_fence_test.go`。
- 服务注入：`internal/server/server.go`、`pkg/cluster/cluster/options.go`、`pkg/cluster/cluster/server.go`。
- 频道存储和排空：`pkg/cluster/channel/storage.go`、`pkg/cluster/channel/history_barrier.go`、`pkg/cluster/channel/history_barrier_test.go`。
- Store：`pkg/cluster/store/history_fence.go`、`pkg/cluster/store/history_fence_test.go`、`pkg/cluster/store/history_lock.go`、`pkg/cluster/store/history_lock_test.go`、`pkg/cluster/store/history_recovery_test.go`、`pkg/cluster/store/message.go`、`pkg/cluster/store/message_event.go`、`pkg/cluster/store/channel_clusterconfig.go`、`pkg/cluster/store/options.go`、`pkg/cluster/store/store.go`、`pkg/cluster/store/store_apply.go`、`pkg/cluster/store/model.go`。
- Raft 队列屏障：`pkg/raft/raftgroup/history_barrier.go`、`pkg/raft/raftgroup/history_barrier_test.go`、`pkg/raft/raftgroup/raftgroup.go`、`pkg/raft/raftgroup/types.go`。
- Pebble：`pkg/wkdb/history_fence.go`、`pkg/wkdb/history_fence_test.go`、`pkg/wkdb/history_lock.go`、`pkg/wkdb/history_lock_test.go`、`pkg/wkdb/message.go`、`pkg/wkdb/wukongdb.go`。
- 说明：`docs/history-fence.md`。
