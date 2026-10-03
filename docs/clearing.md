# 站台清客确认（clearing）

列车到站开门后，站务按**门区**逐块确认"本门区清客完毕"，值班员**统一收口**；
没收口、挂起中、或收口后事实又发生变化（有人闯回、确认过期、代际翻代）时，
发车结论恒为 `no_go`。门控、联锁、发车编排读到的是从同一条哈希链折叠出的
**同一个结论对象**，不存在三份各算一遍的状态。

## 一、两个已定的取舍

### 1. 门区确认顺序：任意顺序确认 + 统一收口（不是顺序一路推到底）

* 站务可并行/乱序确认任意门区（高峰期多人分段清客，缩短停站时间）。
* 但"最后一块确认完"不会自动放行，必须值班员显式 `finalize` 收口。
  收口瞬间对全量事实做一次冻结（生成 `digest`），这是发车前的整体核对点。
* 收口后到真正发车之间，只要有任何事实变化（闯回、过期），`digest` 对不上，
  结论立即回到 `no_go`，必须重新确认、重新收口。

选它而不是"按顺序推到底"的原因：顺序串行在 4~12 个门区的站台上会把停站时间
绑死在最慢的一个人身上，而"任意确认"省掉的时间由"收口 + digest 比对"这道
显式闸门把一致性重新锁回来——安全不依赖确认顺序，只依赖收口那一刻的全量事实。

### 2. 确认超时：默认保留（preserve），可配置整单重来（reset）

* `preserve`（默认）：确认记录**永不删除**，但每条确认有 `confirm_ttl_s`
  有效期。超 TTL 的确认在收口/读结论时被标 `confirm_expired`（软失效），
  值班员只需对过期门区重新确认。适合大客流——人多慢，但不会因一块超时让全站重来。
* `reset`：停站时间跨过一个 TTL 窗口，整单全局代际 `epoch` 加一，所有确认标
  `stale_epoch` 全部失效，必须逐块重来。适合小客流/安全优先线路——"看得太早"
  的确认一律不信。

两者后果不同，因此做成站台级配置（`config.clearing.timeout_policy`），并在
会话起始事件里记下本次会话实际使用的策略，随哈希链永久可查。

## 二、状态机

```
到站开门
  │  session_started (epoch=1)
  ▼
 open ──任意门区确认──► open（可并发确认不同门区）
  │
  │ finalize（全部门区当前+未过期+无挂起）
  ▼
 finalized ──verdict=go（digest 冻结）
  │  │
  │  └──有人闯回──► intrusion_reported ──► suspended（相关门区失效,结论作废）
  │                                        │ resolve_intrusion（现场处置完）
  │                                        ▼
  │                                    open（被闯门区须重新确认→重新收口）
  │
  │ 门控关门+信号放行成功 → clearance_dispatched
  ▼
 dispatched（结论已消费，再发车 => already_dispatched）
```

门区"当前有效确认"的判定（两个条件缺一不可）：

* `confirm_epoch == 当前 epoch` —— 挡住 reset 翻代前的旧确认；
* `confirm_seq > invalidate_seq` —— 挡住本代际内闯入后没重新确认的门区，
  同时正确放行"闯入处置完之后"的重新确认（事件序号必然更靠后）。

## 三、三处事实如何对齐

* **唯一事实源**：`var/state/clearing-<session>-NNNNN.jsonl`，只追加的哈希链。
  每条事件含 `seq`（会话内连续）、`prev_hash`、`hash`（SHA-256，确定性 JSON
  序列化）。改一个字节 / 中间插删一条，`verify` 立即报 `ChainBroken`。
* **当前态**：`SessionState` 完全由事件流 `apply_event` 折叠，不另存"当前态"
  文档——因此不存在"流水和当前态对不上"。
* **结论**：`verdict_of(state)` 是唯一出口，门控（`depart` 链第一道闸）、
  联锁语义、控制台、CLI 都只读它。
* **发车闭环**：门控关门/信号成功后调用 `mark_dispatched` 把"已消费"写进同一条
  链；`Platform._reconcile_clearing` 在发车末尾对账门控（已发车落盘）、清客
  （同 session+digest、已 dispatched）、审计（链完整），不一致 `reconciled=false`。

## 四、并发 / 掉线重连 / 部分失败

* **并发确认**：服务内一把可重入锁串行化所有写命令，锁内"校验→追加→折叠"。
  不同门区并发安全；同一 `request_id` 的并发提交恰好一笔落事件，其余重放。
* **幂等**：每条写命令必须带 `request_id`。同键重放返回**当初那笔**的结果，
  不产生第二条事件。幂等键不能跨会话复用（会被 `ClearingConflict` 拒绝）。
* **掉线重连**：客户端原样重发 request_id 即可；拿不准进度就 GET 快照对齐。
  服务端状态全部可从哈希链 `restore()` 重建，重建前先做链完整性校验。
* **乐观并发**：确认可带 `expected_epoch`、收口可带 `expected_digest`，基于
  旧页面提交会收到 409 `ClearingConflict` 并附最新快照。
* **部分失败恢复**：门控/信号在清客闭环登记之前失败时，结论不会被消费
  （仍 finalized/go），修复后整条发车链可安全重试；成功后结论只能消费一次，
  重复发车收到 `already_dispatched`。

## 五、HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/clearing/{train}` | 会话快照（门区明细、epoch、digest、结论） |
| GET | `/api/clearing/{train}/verdict` | 只读取发车结论 |
| GET | `/api/clearing/{train}/events` | 全部事实事件（历史不可变） |
| POST | `/api/clearing/{train}/confirm` | 确认门区 `{zone_id,request_id,confirmed_by?,expected_epoch?}` |
| POST | `/api/clearing/{train}/intrude` | 闯回挂起 `{request_id,zone_ids?,reason?}` |
| POST | `/api/clearing/{train}/resolve` | 解除挂起 `{request_id}` |
| POST | `/api/clearing/{train}/finalize` | 收口 `{request_id,expected_digest?}` |
| POST | `/api/clearing/{train}/verify` | 哈希链完整性校验 |

到站 `POST /api/arrive` 成功即自动开会话；`POST /api/depart` 以清客 `go` 为
第一道闸。页面见控制台「清客」页。

## 六、命令行验收

```
make demo                       # 既有全链路（已含清客收口）
python -m psdline clear-demo    # 清客专项：并发确认/重连重放/闯回恢复/对账/重启
python -m psdline clear-demo --policy reset --ttl 5
```

## 七、审计事件

`clearing_start`、`clearing_confirm`、`clearing_intrusion`、
`clearing_intrusion_resolved`、`clearing_finalize`、`clearing_depart_held`
（被挡）、`clearing_dispatch`（放行并对账）。审计流水与清客哈希链分别落盘，
一个是业务动作时间线，一个是不可变事实链，凭 session_id + digest 互相对照。

## 八、关键测试

`tests/test_clearing.py`（领域：顺序/收口/闯入/两种超时/幂等/并发/重建/篡改）
与 `tests/test_platform_clearing.py`（集成：三处对账、硬件失败恢复、重启、
未确认不放行）。
