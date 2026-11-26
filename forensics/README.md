# 外键“写入成功、提交被拒”本地取证

针对 `badge_branch`（`badge.branch_id → branch.id`，schema 中声明为
`deferrable-deferred`）的延迟外键，用真实 `lockmere` 二进制复现并固定证据。

## 一键复现（幂等）

```sh
cd /workspace/02-lockmere-20
forensics/run_all.sh
```

每个场景都在 `forensics/runs/<场景>-store/` 用**全新空库**重建，完整行协议帧
落在 `forensics/evidence/<场景>.out`。重复运行结果一致。

| 场景 | 调度文件 | 证据输出 | 证明的事 |
| --- | --- | --- | --- |
| A | `schedules/A_deferred_self_failure.lm` | `evidence/A_*.out` | 延迟检验在 COMMIT 才真正执行并失败；失败后事务存活、可查看、可修复后提交 |
| B | `schedules/B_parent_deleted_by_other_tx.lm` | `evidence/B_*.out` | 写入时父键存在且通过；另一事务先合法删除父键并提交；COMMIT 按最新态复检被拒 |
| C | `schedules/C_rollback_and_reopen.lm` | `evidence/C_*.out` | 失败事务回滚前自可见；回滚后消失；REOPEN 与历史均无该次失败提交 |
| D | `schedules/D_immediate_control.lm` | `evidence/D_*.out` | 同一约束切 IMMEDIATE 后在“写入入口”即拒，对照出阶段差异；保存点修复路径 |

## 两个真实入口，两次不同的检查

写入与提交是 `tx` 模块里两条独立代码路径，约束在两处各评估一次。

### 写入入口（INSERT/UPDATE/DELETE 一条语句）

`Workspace::stage` 落定 intent 后构造 `Evaluator` 并调用 `statement()`
（`src/tx/workspace.rs:113-120`）。对延迟约束，`Evaluator::statement`
直接返回 `Deferred`、`examined = 0`，**不扫描任何行**（`src/constraints/check.rs:122-128`）。
因此即便父键不存在，写入也被接受，intent 进入事务暂存区。

证据（A，`evidence/A_*.out` 事件 id 8）：

```json
{"constraint":"badge_branch","phase":"statement","examined":0,"verdict":"deferred"}
```

### 提交入口（COMMIT）

`State::commit` 先调用 `validate_commit`，**通过之后**才分配 sequence、编码并
`journal.append`、再 `publish`（`src/tx/state.rs:270-303`）。
`validate_commit` 用“最新已提交序列号 `context.latest` + 本事务 pending 叠加”的
视图跑 `Evaluator::commit()`（`src/tx/workspace.rs:205-207`，
投影构造见 `src/constraints/check.rs:172-179`）。外键逐子行查父键集合，
缺失即 `MissingParent`（`src/constraints/check.rs:213-224`）。
失败经 `?` 提前返回，**append/publish/sequence 自增全部不发生**。

证据（A，事件 id 9）：

```json
{"constraint":"badge_branch","phase":"commit","examined":1,
 "verdict":"missing_parent","detail":{"value":7,"child":{"table":"badge","id":"stray"}}}
```

## 判别两种“提交被拒”

两者报错相同（都是 `CONSTRAINT_VIOLATION / badge_branch`），但成因与可修复方式不同。

| 维度 | A：延迟检验本身失败 | B：父键被另一事务合法删除 |
| --- | --- | --- |
| 写入时父键状态 | 全程不存在 | 写入时存在，语句因此合法成立 |
| 语句期事件 | `deferred`（examined 0） | `deferred`（examined 0） |
| 父键消失原因 | 从未存在 | 事务 P 先 COMMIT 删除（B 的 seq2，journal 中 `branch:I2:D` 墓碑） |
| 提交期视图 | 最新态里仍无父键 | 快照 GET 仍读到旧父键，但提交门按最新态判定父键已删 |
| 提交期事件 | `missing_parent` value=7 | `missing_parent` value=2 |
| 修复手法 | 本事务插入缺失父键即可 | 改指向新父键；**重插同一个被删键 2 会先撞 MVCC 陈旧写 `SERIALIZATION`**（该错误终结事务，见下） |

B 的视图分裂是预期语义：快照读 `GET` 用事务快照序列号（`src/tx/state.rs:403-409`），
所以 P 提交删除后 C 的 `GET branch 2` 仍返回旧行；而提交门刻意用最新序列号复检。
提交顺序由历史固定为 seed(seq1) → P 删除(seq2) → C 修复后提交(seq3)，C 被拒时
历史里只有 seq1、seq2，没有 C 的帧（`evidence/B_*.out` 的 `history-mid`）。

## 失败事务仍然存活、可查看

`ConstraintViolation` 不在 `Error::ends_transaction()` 集合内
（`src/error.rs:114-125`），所以提交失败不会调用 `finish`，事务、暂存行、锁都保留。
协议错误帧明确 `"transaction_ended":false`，随后 `SHOW TRANSACTION` 仍返回该
事务且 `writes:1`，`GET` 能读到自己那条未提交子行（A 的 `fk-show-tx`/`fk-read-child`，
C 的 `bad-show-tx`/`bad-self-read`）。

对照：重插被删父键触发的 `SERIALIZATION` 属于 `ends_transaction`，
错误帧 `"transaction_ended":true`，事务被直接关闭，无法再修复——这是另一类失败。

## 重开与历史都不含失败提交（WAL 原始字节取证）

提交门在 append 之前失败，因此失败提交没有任何 journal 帧。

- C 库 `runs/C_rollback_and_reopen-store/journal.v1` 的 `DATA` 之后只有 1 帧（seq1 基线）。
  失败子键 `ghost` 的十六进制 `67686f7374` 在该 journal 中**不存在**：

  ```sh
  grep 67686f7374 runs/C_rollback_and_reopen-store/journal.v1   # 无匹配
  ```

- 形成对照，A 库修复后成功提交，子键 `stray` 的十六进制 `7374726179` **存在**于
  seq2 帧；该帧 receipt 解码为 `c / fk-commit-ok`（成功请求身份），而非被拒的 `fk-commit-fail`。
- C 场景 `REOPEN` 输出 `reopen sequence 1`，重开后 `SHOW HISTORY` 仅 seq1，
  `GET badge 'ghost'` 返回 `null`。目录下无 checkpoint 文件，证明是纯 journal 重放。
- D 库 journal 仅 3 帧：被 IMMEDIATE 拒绝的 `nope` 与被保存点回滚的 `stray` 均无帧，
  只有合法的 `ok1`(seq2)、`good`(seq3) 落盘。

## 源码索引

- 语句期延迟短路：`src/constraints/check.rs:116-134`
- 提交期复检（最新态 + pending 叠加）：`src/constraints/check.rs:136-144, 172-226`
- 写入入口接线：`src/tx/workspace.rs:75-128`
- 提交入口“先校验后 append/publish”：`src/tx/state.rs:248-304`
- 失败不终结事务的判定：`src/error.rs:107-125`
- 快照读 vs 最新态提交门：`src/tx/state.rs:403-409` 与 `src/tx/workspace.rs:197-208`
