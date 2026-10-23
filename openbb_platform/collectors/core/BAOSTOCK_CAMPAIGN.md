# BaoStock 独立采集 campaign v2

本入口只接收显式 literal tasks，保存 provider 的 SDK 表示及私有恢复事实；不读取交易日历、中央目录、job DB、watermark 或 consumer ACK，不做业务归一化、发布或生产调度。

**v2 改用逐页持久确认，不再把多页合并成一个 ProcessClient 返回值。** 旧 `baostock_acquisition` CLI、native models、通用 ProcessClient、raw-acquisition-v1 均不变。没有 SDK build、安装或生产启用含义。

## 入口和兼容

```sh
export OPENBB_AUTO_BUILD=false PYTHONDONTWRITEBYTECODE=1
python -B -m openbb_collector_core.baostock_campaign compile \
  --plan literal-plan.json --manifest new-manifest \
  --max-attempts 3 --request-budget 100 --backoff-seconds 60
python -B -m openbb_collector_core.baostock_campaign run \
  --manifest new-manifest --root private-evidence --run explicit-new-run --slot explicit-slot
python -B -m openbb_collector_core.baostock_campaign verify \
  --root private-evidence --handoff exact-handoff-path --sha256 retained-handoff-hash
```

`literal-plan.json` 只有 `{"tasks": [...]}`，tasks 使用既有 raw-v1 literal task 形状。每页最多八个 task、每 task 最多两个 request。编译只枚举原顺序并建立 immutable hash-linked pages；没有生产 universe/date discovery。

支持 HistoryKDataPlus、AdjustFactor、StockBasic、TradeDates 四个 BaoStock native operation 名称，实际 dispatch 对应已注册的 `...Source` fetcher。非法/隐式参数拒绝，不 trim、改大小写或补日期。真实链路为新 acquisition 专用 spawn worker → `executor_for('baostock')` → QueryExecutor → Source fetcher → provider SDK session。只有 `EvidenceSinkV1` 可信 in-process capability 能启用 sink；literal 参数不能携带 callback 或输出路径。

新 manifest/authority 为 `baostock-campaign-v2`；authority 明确绑定 capture kind 和资源配置。旧 v1 manifest 或同 run key 的旧 namespace 返回 `legacy_capture_requires_new_run`，不自动续用已知可能丢 prefix 的聚合 backend，也不迁移旧 authority。旧 raw bodies、旧 handoff 可通过 `verify`/原 raw reader **只读验证**。需新采集时由调用者明确提供新 manifest/run/slot；这是 fresh acquisition，不是生产身份迁移。普通无 sink Source 调用保留原聚合兼容行为，不能拿它声称具备 v2 campaign 的逐页保证。

## 源值与 codec

普通 JSON 值/类型保持原样，包括 float/bool SDK error_code、重复 field、空行、对象行、空字符串、decimal strings。success 只接受 **type=str 且值='0'**；保存 body 与 source success 分开。

沿用同一个 evidence codec：

- 普通 envelope 为 `baostock-source-evidence-v1`。
- 遇到非有限 binary64 使用 `baostock-source-evidence-v2`，该节点存 authored marker `$float64`，顶层 `value_types` 用绝对 path、type=`float64`、16 位大端 IEEE754 hex 指向它。NaN payload/sign、±Inf 可逆；原始同名字符串/字典没有 sidecar 引用便仍是原始值，不发生 tag collision。
- `decode_evidence` 同时读 v1/v2，不改变输入或旧 bodies；只允许 source value 路径，拒绝重复路径、错 marker、错误 bits。stream 的 page 额外保留实际 SDK `cur_page_num` 为 `sdk_page`，没有则 null。
- 支持 exact built-in scalar/list/str-key dict，整数最多 4096 bits，容器嵌套最多 16 层（预留旧 raw-v1 的 envelope depth）。其它对象、cycles、超域子树以显式 `unrepresentable` sidecar/unknown sentinel 表示，保留其它行，标记 incomplete；不宣称能重建任意 Python 对象，不调用 repr、deepcopy hook 或读取秘密属性。
- 不把异常值 null/drop/str 化。SDK `error_msg`、任意异常文字和 credentials 不进 evidence。SDK 表示不是 wire bytes。

v2 codec 改变新 body bytes，故 body/fragment/receipt/handoff digest 随之改变；普通 JSON envelope 不因 marker 系统改变值/类型。新 page-capture 包装和 clocks 本身也形成新格式 digest。**不重写、不重新 hash 安装任何既有 body。**

## 新私有页证据

`baostock-page-capture-v1/<attempt-id>/` 不冒充 raw-v1。attempt-id 绑定 campaign/run/manifest page/original task+generation/request/attempt 经 campaign task identity 进入完整 task hash。

- `authority.json`：完整 task、literal、identity、codec、limits、package provenance、首次 receipt clock、restart_of/refetch；source_time=null，不伪造源时钟。
- `page-NNN.begin.json`：observation index、trigger、receipt clock、previous state hash、previous page root。`next_raise`/`next_return` 是 SDK 后状态，不冒充另一份 wire response。
- `page-NNN.body`：一个 observation 的完整 evidence envelope，逐 token 编码；字符串按 4096 字符编码分片，**没有截断源字符串**。
- `page-NNN.fragments.jsonl`：顺序/offset/长度/fragment hash/前 record hash。每个 body fragment flush+fsync 后再 journal flush+fsync，才发 FRAGMENT_ACK。
- `page-NNN.receipt.json`：begin hash、完整 body hash、fragment root、字节/fragment counts、previous page root。安装并验证后才 PAGE_ACK；provider 收到后才可能调用同一 ResultData.next()。
- `terminal.json`：source/operational outcome、cleanup、计数、最终 page root；terminal 可见不代表成功。
- `eligible.json`：terminal hash 和 last page root，最后资格屏障；`cancel.json` 是 durable veto。
- `.capacity`：本次 producer 的预分配空间 credit，**不是源证据**；随写入释放、正常退出释放余量。异常退出的余量只由持有 campaign lock 的 producer 在 exact attempt proof 后释放；reader 不创建、修复、删除或 fsync 任何 evidence。

实现不增加可变 per-attempt control cache：reader 按固定最多 128 个 observation 索引读取 exact paths，验证连续 receipts/pending prefix。没有 lifetime archive enumeration。页 ACK 当前实现还重读该 attempt 已保存的 pages；成本有上界，但不是 O(1) 每页，也不是生产吞吐优化。

`read_capture(root, task, expected=capture_sha256)` 验证 exact attempt；`read_page` 解码一个已验证页。handoff 通过 kind/locator/capture root/terminal hash/last page root 绑定每个 attempt。pending suffix 不截短，不伪造 complete；pending 的 acknowledgement=unknown，不把可读 journal 尾行冒充已收到 ACK。torn journal 尾部仅使其后的字节不确定，已完整记录的 prefix 保留。完整页 digest/record/order/symlink 冲突拒绝，不自动修复。

若 eligibility 安装的目录 fsync 失败，progress 仍停在 active reservation。producer 恢复必须重读 exact capture，并重新同步**已有** eligibility marker 和目录屏障才能采用；失败则继续拒绝、零新 source calls，不重建缺失证据。无 terminal/eligible 的死会话是 incomplete。

## 资源、取消与不可避免的限制

默认单 attempt 最多 **134217728 encoded body bytes、128 observations**。binary frame payload 最大 65536 bytes，application frame 最大 65537 bytes；控制消息 4096 bytes，authority 262144 bytes。这些是支持资源边界，不是 vendor 单页大小保证；原 8 MB/page、24 MB/aggregate 不再是 campaign admission依据，未提高通用 ProcessClient 的 50 MB 限额。

在任何 source effect 前校验 limits、literal/authority，statvfs 检查并 posix_fallocate 预留 body、bounded fragment journals、page controls 和 slack 的空间 credit。不是声称能排除另一进程抢占磁盘或真实 ENOSPC/EDQUOT。每 fragment 在写入前检查累计 encoded quota；超域停止并保留 prior pages 及已确认的当前 fragment prefix。支持域内 7 MB+44 MB、25 MB 单页完整保存；任意大 SDK 对象、有限内存外返回或已失败物理存储不可能承诺无损。

source-call deadline 默认 180 秒、页 durable-drain allowance 120 秒、整个 attempt 900 秒。合法 terminal summary 不等于进程已退出：正常退出按 `min(原 attempt 起点 + attempt_seconds, 退出等待起点 + drain_seconds)` 的绝对期限等待，最多每 0.05 秒重新检查 stop；0.05 秒只是取消轮询间隔，不是成功退出门槛，不重置 attempt 时钟。必须实际正常 exit 0、cleanup ok 且无取消/未完成页才成功；超期或接受 stop 后被强制终止，即使 signal handler 以 exit 0 结束也不成功。

SIGTERM/SIGINT latch 阻止后续新 source effect；已返回页可以在 bounded drain 内继续落盘，再拒绝 next。deadline/hard death 只保证已 durable ACK 的 fragment/page，不保证尚在 SDK 内存或管道里的字节。超期、取消或异常需要回收时，仍存活的 worker 依次 terminate、最多 join 2 秒；仍存活才 kill、最多再 join 2 秒。每次回收至多这两个等待（共 4 秒），可能超过业务截止，不会在 finally 重新赠予一套等待；已经退出的 worker 立即 reap，不为凑足 grace 等待。无法确认 reap 则报错，不授予成功。上述是请求给 OS 的等待上界，不是调度、信号调用、不可中断内核 I/O 或最终持久化的硬实时墙钟保证。源异常与 sink/文件系统异常分开；cleanup 不覆盖 primary operational error 或 cancellation。

同一活会话只有一次 query，后续为同一 ResultData.next()。SDK 未提供可信跨 session resume token：**死会话的新 attempt 可以重取不完整 request 的 prefix**。新 authority 记录 restart_of、refetch=true、新 clock，不能把两个会话拼成一个连续响应；成功的其它 REQUESTS 仍零重取。CLI 的 refetch/refetch_attempts 是本次 invocation 的 restart dispatch 事实，retry_semantics 明示该限制。

source_calls 是 campaign dispatch 数；sdk_query_calls/sdk_next_calls 来自返回的 provider summary。worker 死亡/未交 summary 时输出 null，而不是把 admission 当成已知实际调用数；每 attempt metrics 另留 source_*_admissions。offline SDK 日志独立计量真实 query/next。completed replay 不增加这些调用。

## Campaign 状态约束

progress 的自身 checksum 不是 authority：page0 必须 completed=0、无 prior handoff、cursor 等于 immutable first hash、reserved 精确等于 current attempts。后页绑定最近 immutable handoff 的 completed/reserved totals 与 next cursor；previous failed attempts 加 current reservations 都不能退款。精确 current handoff lookup 拒绝已封页的 progress rewind，并允许 install-before-cursor crash 的相同 bytes 重放。

complete 必须 terminal page、精确 completed total、空 active/entries 和有效 handoff，不能只因 pending 算式等于零。每个 reservation 在 source 前落盘；预算/attempt cap/backoff 跨 invocation 保持；取消 veto 不因 restart 清除。campaign 另以 immutable `cancel.json` 绑定取消事实，单独把 progress.cancelled 改回 false 也会拒绝。耗尽 run 的新额度必须显式新 run，且 fresh-fetch，不是扩容旧身份。首次 authority 安装后 progress 缺失仍 fail closed，不自动建立。

新 `baostock-campaign-handoff-v4` 封存 page hash/next hash、prior/current completed 和 reserved totals、全部成功/失败 links，并要求独立的本页 admission。v4 在此之上增加 `outcome`（`complete` | `closed_with_failures`）与 `failures`（按 entry 顺序的 `{index, attempt, status, locator, terminal_sha256}`），二者只能由 entries 推导，读取端重新推导并逐字段比对，不一致返回 `handoff_failure_binding`，不修复。旧 v2/v3 handoff 按原格式只读验证（仍要求最后 link 为 success），不重写、不静默升级；v3 与 v4 页都需要本页 admission。仅重读最后 handoff 的 completed replay 表示历史 acquisition 完成，不是重审所有历史 body；消费旧页需保留 expected handoff hash 并 exact verify。单一 progress restatement 可检测；协调改写全部 authority/receipts/hashes 不在无签名本地契约的防御范围。

### 死信页关闭（v2.1）

翻页条件不再要求整页 success：当页内每个 entry 都是终态时封页。终态 = 最后 link 为 success，或最后 link 为 failed 且 `entry.attempts >= policy.max_attempts`（即策略重试额度已耗尽）。`unknown`/`cancelled` 不是终态，页不关闭。这样一条耗尽重试的请求只把本页封成 `closed_with_failures`，不再永久阻塞后续所有页；失败的 attempt、locator 和 terminal hash 仍指向真实保留证据，且必须能通过 `linked_capture` 校验。success 语义不变：只有 success 的请求才算采到数据，失败一律显式列入 `failures`；`completed_requests` 计入已封页的全部请求数，因此死信页之后游标继续前进。v4 允许最后 link 为 failed，但仍要求 `entry.attempts == len(entry.links)`（不存在未完成 reservation）。

### 登录限流熔断与请求节流（v2.1）

一次 capture 的 terminal summary `reason` 为 `login_failed` 时，本次 invocation 立即结束 entry 循环（不再消耗同页后续请求的预算），并在 run 目录（`baostock-campaign-v2/<digest(run)>/`，与 `authority.json` 同级）原子写入有 checksum 的 `backoff.json`：`{version: baostock-campaign-backoff-v1, binding, until_ns, reason: login_failed}`。该文件不属于 progress 键空间，`state_check` 的键集合不变。合法 binding 且 `until_ns > now` 时 `run` 直接返回 `partial` 且 `source_calls=0`，不调用源；过期或 binding 不匹配则忽略，已记录的 attempt 不退款。窗口默认 300 秒，可由 `OPENBB_CAMPAIGN_LOGIN_BACKOFF_SECONDS` 覆盖（非整数或越界拒绝启动）。同一 invocation 内相邻 `capture_process` 之间另按 `OPENBB_CAMPAIGN_PACE_SECONDS`（默认 0）节流，取消发生在间隔内不会留下新 reservation。

### 最后取消边界与本页消费资格

新页的 immutable handoff 只是候选证据；`admission-NNNNNN.json` 是有 checksum 的有界 mutable control，版本 `baostock-campaign-page-admission-v1`，精确绑定 campaign authority digest、页号和 handoff digest。`pending` 不授资格；消费必须使用 producer 成功返回的 expected handoff，且 exact verify 检查 `eligible` 和无本页 `veto-NNNNNN.json`。单独可见的 eligible 文件不证明其写入屏障曾成功。veto 是 immutable 的本页取消事实；run `cancel.json` 继续约束同 run 零 source replay。不删除 handoff、attempt 或源值，不修改 clocks、预算或旧成功页的资格。

整个决策使用既有 root `.lock` 的 producer 独占 ownership。v3 exact verifier 只读打开同一把锁并取 nonblocking shared flock；producer 尚未释放时返回 `acquisition_busy`（CLI 75），不抢先消费、不补写或 fsync。检查 exact expected、原 evidence、authority/path、对应 eligible record 和 page veto。历史 v1/v2 兼容入口不依赖新记录。

**最后取消接受 cutoff 是 final snapshot 构建完成之后的最后一次 stop Event 采样。** 它位于 worker cleanup、handoff 安装及屏障、pending admission 的写入/cleanup、progress replacement/fsync/cleanup 之后，仍在 producer ownership 内。cutoff 前接受的取消：先保存本页 veto，再保存 run cancel/progress，不发布 eligible。cutoff 后的 signal（包括最终 eligibility decision 写入、返回、unlock 期间）不改变本次已选定决策；CLI 保留 handler 到输出完成，不再次把结果改称 cancelled。这样 cancelled 的当前候选即使返回真实 handoff hash，也不能通过消费验证。更早已合法完成的页不被后页取消追溯撤销。

pending 在 handoff 安装返回后、cursor 变化/取消修正之前保存。重启发现 pending，按未完成最终决策 fail closed，持久取消并零 source calls；不会静默重新批准。在 handoff install 内死亡、尚无 pending/cursor advancement 的原恢复窗口仍允许 exact evidence 重放，不重取源；失败安装路径接受取消时也尝试保留 pending denial，独立于可能失败的 veto。已推进 cursor 却缺 admission 同样不自动授资格。取消持久化失败保留 exact primary/cause 并报告 operational failure；持续失败不授当前页资格。若硬杀之前没有任何取消/denial 事实成功保存，重启不能推断丢失的内存信号；不承诺掉电保证。

**最终 admission 的存储屏障不受取消 cutoff 豁免。** 新 eligible 写入之后，以及重启读取已存在且内容相同的 eligible 之后，producer 在既有独占 ownership 内精确校验 decision 与已验证 handoff，重新同步 admission 文件、run 目录、version 目录、private root 及包含 root 的父目录，全部成功才返回该 handoff 或继续后页。此 bounded barrier 不扫描历史，不重写 decision/evidence/clocks，不退款或重取源。持续 file/directory/ancestor 故障继续抛 operational failure；故障清除后可原字节零 source 恢复。该存储边界以上的外部目录 provisioning、掉电、NAS/Windows 不在本地验证承诺内。

reader 不补 fsync，也不能从可见文件推断另一进程失败或未返回的 fsync acknowledgement。失败 invocation 不授予新的成功 expected；从失败 artifact 自算 hash，或从失败 invocation 的文件直接枚举 hash，不是受支持的成功 handoff。已有成功返回的旧页不因后续 invocation 存储失败而追溯失效。相同字节没有另加“证书”：证书自身仍有 rename/fsync 窗口，不能证明其自身持久化完成。

正常新页仍写 pending/eligible 两个 control；最终 confirmation 额外做一次有界 admission read、一次 file fsync 和四次 directory fsync，无 source body reread。已存在 eligible 的 producer replay 同样做这五次同步，不以同内容早退；Control metrics 计入这些成功完成的同步与 admission read。取消额外保存本页 veto。v3 verify 的只读 shared root lock、authority/admission 检查和版本选择用 handoff 读取不变。现有 root 粒度锁意味着后页 producer 活跃时，旧 v3 页的 reader 也可能 busy；没有新中央库或并发 publisher。


## 计量与验收

Control metrics 只覆盖 campaign Control read/progress write；page-capture metrics 单独记录 body/journal 写入、fragment fsync、元数据安装/字节、最大 frame、elapsed、worker peak RSS（本 Linux/WSL 环境的 KiB）。fragment_fsyncs 不包括 authority/receipt/directory/capacity fsync。posix_fallocate 与 repeated readback 都有真实成本，不能用小夹具证明 NAS、Windows、断电或生产量级吞吐。

实现者离线 fixtures 只合成 SDK 返回，CLI、spawn、selected QueryExecutor、IPC、真实临时文件系统不替代。独立 SPEC → QUALITY → 父级实际执行仍是不同 gate；源码实现/测试绿色不是自批、安装或部署许可。生产 literal export 许可未获批，数据库/服务/调度器未触及。
