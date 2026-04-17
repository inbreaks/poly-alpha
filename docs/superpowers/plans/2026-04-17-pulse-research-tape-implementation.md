# Pulse Research Tape Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 `pulse` runtime 补齐 research-grade replay 数据链路，让本地可以用同一份真实 tape 回答“信号定义是否有问题”和“退出机制是否能赚钱”。

**Architecture:** 复用现有 `PulseAuditRuntime -> AuditWriter -> raw segments` 主链，不重做存储系统；在 `polyalpha-audit` 扩展 `PulseMarketTape` / `PulseSignalSnapshot` 契约与 `execution_context.json`，在 `polyalpha-pulse` runtime 为全市场输入流和每次 `evaluation_attempt` 发射事件，再由 `polyalpha-cli/src/pulse.rs` 写入原始审计文件。

**Tech Stack:** Rust workspace, existing `AuditWriter` / `PulseAuditSink` / `PulseRuntime`, `serde`, `rust_decimal`, targeted `cargo test -p polyalpha-pulse`, `cargo test -p polyalpha-cli`, `cargo test -p polyalpha-audit`.

---

## File Structure

- Modify: `crates/polyalpha-audit/src/model.rs`
  Responsibility: 扩展 `PulseMarketTapeAuditEvent` / `PulseSignalSnapshotAuditEvent` 字段，新增 `PulseExecutionContext` 数据结构。
- Modify: `crates/polyalpha-audit/src/writer.rs`
  Responsibility: 为 session sidecar 写入 `execution_context.json`。
- Modify: `crates/polyalpha-pulse/src/audit.rs`
  Responsibility: 扩展 audit sink，支持 execution context 与 richer tape / evaluation rows。
- Modify: `crates/polyalpha-pulse/src/runtime.rs`
  Responsibility: 为全市场 book update 发射 market tape，为每次 evaluation attempt 发射 signal snapshot。
- Modify: `crates/polyalpha-cli/src/pulse.rs`
  Responsibility: 启动 audit runtime 时写入 execution context，并把新增 pulse audit records 下沉到 raw segments。
- Modify: `crates/polyalpha-pulse/src/lib.rs`
  Responsibility: 导出必要的新 audit 类型（如果需要）。
- Test: `crates/polyalpha-audit/src/model.rs`
- Test: `crates/polyalpha-audit/src/writer.rs`
- Test: `crates/polyalpha-pulse/src/audit.rs`
- Test: `crates/polyalpha-pulse/src/runtime.rs`
- Test: `crates/polyalpha-cli/src/pulse.rs`

### Task 1: 扩展审计数据契约，先把 research tape 的 schema 定死

**Files:**
- Modify: `crates/polyalpha-audit/src/model.rs`
- Test: `crates/polyalpha-audit/src/model.rs`

- [ ] **Step 1: 先写失败测试，锁定新增字段的序列化契约**

在 `crates/polyalpha-audit/src/model.rs` 增加测试，至少覆盖：

```rust
#[test]
fn pulse_market_tape_serializes_sequence_and_level_deltas() {
    // market tape 必须包含 ts / recv_ts / sequence / venue / instrument / levels。
}

#[test]
fn pulse_signal_snapshot_serializes_pricing_and_exit_fields() {
    // evaluation row 必须包含 fair_prob / net_edge / expected_pnl / target / timeout / rejection。
}

#[test]
fn pulse_execution_context_serializes_static_runtime_constraints() {
    // execution_context.json 必须能完整写出 fee / tick / qty_step / timeout / provider / venue。
}
```

- [ ] **Step 2: 运行测试，确认现在是红灯**

Run:

```bash
cargo test -p polyalpha-audit pulse_market_tape_serializes_sequence_and_level_deltas -- --nocapture
cargo test -p polyalpha-audit pulse_signal_snapshot_serializes_pricing_and_exit_fields -- --nocapture
cargo test -p polyalpha-audit pulse_execution_context_serializes_static_runtime_constraints -- --nocapture
```

Expected:

- FAIL，因为当前 schema 缺少这些字段或类型还不存在

- [ ] **Step 3: 修改 model，补齐 research tape schema**

在 `crates/polyalpha-audit/src/model.rs`：

- 扩展 `PulseMarketTapeAuditEvent`
  - 加入 `ts_exchange_ms` / `ts_recv_ms` 统一命名
  - 保留 `sequence`
  - 增加 `update_kind`
  - 为 Poly / Binance / Deribit 的增量或快照字段保留位
- 扩展 `PulseSignalSnapshotAuditEvent`
  - 补 `fair_prob_yes`
  - 补 `entry_price`
  - 补 `net_edge_bps`
  - 补 `expected_net_pnl_usd`
  - 补 `target_exit_price`
  - 补 `timeout_exit_price`
  - 补 `timeout_loss_estimate_usd`
  - 补 tape join 键：`poly_yes_sequence_ref` / `poly_no_sequence_ref` / `cex_sequence_ref` / `anchor_sequence_ref`
- 新增 `PulseExecutionContext`

- [ ] **Step 4: 跑 model 测试，确认绿灯**

Run:

```bash
cargo test -p polyalpha-audit pulse_market_tape_ -- --nocapture
cargo test -p polyalpha-audit pulse_signal_snapshot_ -- --nocapture
cargo test -p polyalpha-audit pulse_execution_context_ -- --nocapture
```

Expected:

- 新增 schema 测试 PASS

### Task 2: 让 AuditWriter 支持 session sidecar，落地 execution_context.json

**Files:**
- Modify: `crates/polyalpha-audit/src/writer.rs`
- Test: `crates/polyalpha-audit/src/writer.rs`

- [ ] **Step 1: 先写失败测试，锁定 execution_context.json 会被写进 session 目录**

在 `crates/polyalpha-audit/src/writer.rs` 增加测试：

```rust
#[test]
fn writer_persists_execution_context_sidecar() {
    // create writer -> write execution context -> session_dir/execution_context.json exists
}
```

- [ ] **Step 2: 运行测试，确认红灯**

Run:

```bash
cargo test -p polyalpha-audit writer_persists_execution_context_sidecar -- --nocapture
```

Expected:

- FAIL，因为 `AuditWriter` 还没有 sidecar 写接口

- [ ] **Step 3: 给 writer 增加 sidecar 写接口**

在 `crates/polyalpha-audit/src/writer.rs`：

- 增加 `write_execution_context(&PulseExecutionContext)` 或等价方法
- 输出路径固定为 `session_dir/execution_context.json`
- 使用和 `manifest` / `summary` 同样的原子写文件方式

- [ ] **Step 4: 运行 writer 测试，确认绿灯**

Run:

```bash
cargo test -p polyalpha-audit writer_persists_execution_context_sidecar -- --nocapture
```

Expected:

- PASS，且 JSON 可读

### Task 3: 扩 PulseAuditSink，确保 runtime 产出的 research 事件能进 raw

**Files:**
- Modify: `crates/polyalpha-pulse/src/audit.rs`
- Test: `crates/polyalpha-pulse/src/audit.rs`

- [ ] **Step 1: 先写失败测试，锁定 sink 会记录 market tape / signal snapshot / execution context**

在 `crates/polyalpha-pulse/src/audit.rs` 增加测试：

```rust
#[test]
fn audit_sink_tracks_market_tape_for_non_session_market_updates() {
    // 没有 active session 时，market tape 也必须被记录。
}

#[test]
fn audit_sink_tracks_signal_snapshot_for_rejected_evaluation() {
    // admission rejected 也必须写 signal snapshot。
}
```

- [ ] **Step 2: 运行测试，确认红灯**

Run:

```bash
cargo test -p polyalpha-pulse audit_sink_tracks_market_tape_for_non_session_market_updates -- --nocapture
cargo test -p polyalpha-pulse audit_sink_tracks_signal_snapshot_for_rejected_evaluation -- --nocapture
```

Expected:

- FAIL，或字段断言失败

- [ ] **Step 3: 最小扩展 sink**

在 `crates/polyalpha-pulse/src/audit.rs`：

- 保持 `record_market_tape(...)`
- 保持 `record_signal_snapshot(...)`
- 如有需要，补 execution context 相关缓存或 passthrough 类型
- 确保 `records()` 返回的 payload 不丢字段

- [ ] **Step 4: 跑 audit sink 测试，确认绿灯**

Run:

```bash
cargo test -p polyalpha-pulse audit_sink_tracks_ -- --nocapture
```

Expected:

- audit sink 相关测试 PASS

### Task 4: 为全市场 book update 发射 market tape，而不是只给 active session 录 book tape

**Files:**
- Modify: `crates/polyalpha-pulse/src/runtime.rs`
- Test: `crates/polyalpha-pulse/src/runtime.rs`

- [ ] **Step 1: 先写失败测试，锁定“无 session 时也必须有 market tape”**

在 `crates/polyalpha-pulse/src/runtime.rs` 增加测试：

```rust
#[test]
fn runtime_records_market_tape_for_poly_and_cex_updates_without_active_session() {
    // 只要 observed book 更新，就要写 market tape。
}
```

- [ ] **Step 2: 运行测试，确认红灯**

Run:

```bash
cargo test -p polyalpha-pulse runtime_records_market_tape_for_poly_and_cex_updates_without_active_session -- --nocapture
```

Expected:

- FAIL，因为当前 runtime 只在 active session 上写 `PulseBookTape`

- [ ] **Step 3: 在 runtime 中为全市场发射 market tape**

在 `crates/polyalpha-pulse/src/runtime.rs`：

- 新增 `record_market_tape_for_snapshot(...)`
- 在 Poly yes/no 与 CEX perp book 更新路径里调用
- market tape 必须写：
  - asset
  - symbol
  - venue / instrument
  - `ts_exchange_ms`
  - `ts_recv_ms`
  - `sequence`
  - 当前 top-N levels

- [ ] **Step 4: 跑 runtime tape 测试，确认绿灯**

Run:

```bash
cargo test -p polyalpha-pulse runtime_records_market_tape_for_poly_and_cex_updates_without_active_session -- --nocapture
```

Expected:

- PASS，且 `runtime.audit_records()` 中能看到 `PulseMarketTape`

### Task 5: 为每一次 evaluation attempt 发射 signal snapshot，而不是只在最终 session 上留摘要

**Files:**
- Modify: `crates/polyalpha-pulse/src/runtime.rs`
- Test: `crates/polyalpha-pulse/src/runtime.rs`

- [ ] **Step 1: 先写失败测试，锁定 rejected evaluation 也必须有完整特征**

在 `crates/polyalpha-pulse/src/runtime.rs` 增加测试：

```rust
#[test]
fn runtime_records_signal_snapshot_for_rejected_candidate_with_pricing_fields() {
    // 即便 admission rejected，也必须留下 fair_prob / edge / expected_pnl / rejection_reason。
}

#[test]
fn runtime_records_signal_snapshot_sequence_refs_for_joining_back_to_tape() {
    // signal snapshot 必须保留各腿 sequence ref。
}
```

- [ ] **Step 2: 运行测试，确认红灯**

Run:

```bash
cargo test -p polyalpha-pulse runtime_records_signal_snapshot_for_rejected_candidate_with_pricing_fields -- --nocapture
cargo test -p polyalpha-pulse runtime_records_signal_snapshot_sequence_refs_for_joining_back_to_tape -- --nocapture
```

Expected:

- FAIL，因为当前 runtime 没有稳定发射 `PulseSignalSnapshot`

- [ ] **Step 3: 在 evaluation 路径里发射 signal snapshot**

在 `crates/polyalpha-pulse/src/runtime.rs`：

- 找到每次 pricing / detector / admission 尝试的主路径
- 无论最终是否入场，都写一条 `PulseSignalSnapshot`
- 填充：
  - fair prob / entry / edge / expected pnl
  - target / timeout / timeout loss
  - detector primitives
  - freshness / transport / expiry
  - tape join 键

- [ ] **Step 4: 跑 runtime signal 测试，确认绿灯**

Run:

```bash
cargo test -p polyalpha-pulse runtime_records_signal_snapshot_ -- --nocapture
```

Expected:

- signal snapshot 相关测试 PASS

### Task 6: Deribit anchor 输入也要进入 research tape

**Files:**
- Modify: `crates/polyalpha-pulse/src/runtime.rs`
- Test: `crates/polyalpha-pulse/src/runtime.rs`

- [ ] **Step 1: 先写失败测试，锁定 anchor 更新会写 market tape**

在 `crates/polyalpha-pulse/src/runtime.rs` 增加测试：

```rust
#[test]
fn runtime_records_anchor_market_tape_for_deribit_inputs() {
    // Deribit anchor update 必须进入 research tape，而不是只更新内存 fair prob。
}
```

- [ ] **Step 2: 运行测试，确认红灯**

Run:

```bash
cargo test -p polyalpha-pulse runtime_records_anchor_market_tape_for_deribit_inputs -- --nocapture
```

Expected:

- FAIL，因为当前 runtime 只保留 fair prob sample，不写 raw anchor tape

- [ ] **Step 3: 在 anchor update 路径增加 tape 记录**

实现要求：

- 在 anchor provider / router 更新被 runtime 消费时，落 `PulseMarketTape`
- 至少写：
  - instrument
  - mark iv
  - best bid / ask
  - greeks
  - underlying index price
  - exchange / recv timestamp

- [ ] **Step 4: 跑 anchor tape 测试，确认绿灯**

Run:

```bash
cargo test -p polyalpha-pulse runtime_records_anchor_market_tape_for_deribit_inputs -- --nocapture
```

Expected:

- PASS

### Task 7: 在 CLI audit runtime 中落 execution_context，并把新增 payload 正常写进 raw segments

**Files:**
- Modify: `crates/polyalpha-cli/src/pulse.rs`
- Test: `crates/polyalpha-cli/src/pulse.rs`

- [ ] **Step 1: 先写失败测试，锁定 pulse audit session 会生成 execution_context.json 与 richer raw**

在 `crates/polyalpha-cli/src/pulse.rs` 增加测试：

```rust
#[tokio::test]
async fn pulse_audit_runtime_writes_execution_context_sidecar() {
    // start audit runtime 后必须能看到 execution_context.json
}

#[tokio::test]
async fn pulse_audit_runtime_flushes_market_tape_and_signal_snapshot_events() {
    // sync_runtime_state 后 raw event 文件里必须能看到两类 payload。
}
```

- [ ] **Step 2: 运行测试，确认红灯**

Run:

```bash
cargo test -p polyalpha-cli pulse_audit_runtime_writes_execution_context_sidecar -- --nocapture
cargo test -p polyalpha-cli pulse_audit_runtime_flushes_market_tape_and_signal_snapshot_events -- --nocapture
```

Expected:

- FAIL，因为当前 CLI audit runtime 只写 summary / checkpoint / existing pulse records

- [ ] **Step 3: 实现最小闭环**

在 `crates/polyalpha-cli/src/pulse.rs`：

- `PulseAuditRuntime::start(...)` 时组装 `PulseExecutionContext`
- 调 `AuditWriter` sidecar 写入
- 保持 `append_new_pulse_records(...)` 能完整写入扩展后的 payload

- [ ] **Step 4: 跑 CLI pulse 审计测试，确认绿灯**

Run:

```bash
cargo test -p polyalpha-cli pulse_audit_runtime_ -- --nocapture
```

Expected:

- 新增 audit runtime 测试 PASS

### Task 8: 做端到端验收，证明导出的本地 session 已经足够分析信号与退出

**Files:**
- Modify: 如前述文件
- Test: targeted commands only

- [ ] **Step 1: 本地单元测试全跑**

Run:

```bash
cargo test -p polyalpha-audit -- --nocapture
cargo test -p polyalpha-pulse --lib -- --nocapture
cargo test -p polyalpha-cli pulse_audit_runtime_ -- --nocapture
```

Expected:

- 全部 PASS

- [ ] **Step 2: 服务器重新构建并跑一轮 live-mock**

Run:

```bash
ssh ubuntu@43.133.196.169 'bash -lc "cd /home/ubuntu/data/poly-alpha && cargo build -p polyalpha-cli"'
ssh ubuntu@43.133.196.169 'bash -lc "systemctl --user stop polyalpha-live-mock-fresh.service 2>/dev/null || true && systemd-run --user --unit=polyalpha-live-mock-fresh --description=\"polyalpha live pulse fresh\" --working-directory=/home/ubuntu/data/poly-alpha --service-type=simple --property=Restart=always --property=RestartSec=5s --collect /home/ubuntu/data/poly-alpha/target/debug/polyalpha-cli live pulse run --env multi-market-active.fresh --assets btc,eth --executor-mode mock"'
```

Expected:

- service active (running)

- [ ] **Step 3: 压缩并拉取新 session，确认 raw 中已经出现 market tape / signal snapshot / execution context**

Run:

```bash
ssh ubuntu@43.133.196.169 'bash -lc "cd /home/ubuntu/data/poly-alpha && ./target/debug/polyalpha-cli audit session-summary --env multi-market-active.fresh --format json"'
```

然后：

- 找到最新 `session_id`
- 压缩对应 session 目录
- 下载到本地
- 检查：
  - `execution_context.json`
  - `raw/*market-tape*`
  - `raw/*evaluation*` 或带 `pulse_signal_snapshot` 的事件分片

- [ ] **Step 4: 本地做最小研究检查**

Run:

```bash
python3 tools/check_pulse_research_export.py <downloaded-session-dir>
```

Expected:

- 能输出：
  - market tape 事件条数
  - evaluation rows 条数
  - 非空 `fair_prob_yes` / `expected_net_pnl_usd` 条数
  - 可 join 回 tape 的 sequence refs 覆盖率

- [ ] **Step 5: Commit**

```bash
git add crates/polyalpha-audit/src/model.rs \
  crates/polyalpha-audit/src/writer.rs \
  crates/polyalpha-pulse/src/audit.rs \
  crates/polyalpha-pulse/src/runtime.rs \
  crates/polyalpha-cli/src/pulse.rs \
  docs/superpowers/specs/2026-04-17-pulse-research-tape-design.md \
  docs/superpowers/plans/2026-04-17-pulse-research-tape-implementation.md
git commit -m "feat: add pulse research tape capture"
```

## Self-Review

- Spec coverage:
  - 全量市场 tape：Task 4 + Task 6
  - 全量 evaluation rows：Task 5
  - execution context：Task 2 + Task 7
  - 本地可分析导出：Task 8
- Placeholder scan:
  - 没有保留 TBD / TODO；所有任务都写了测试、命令和目标文件
- Type consistency:
  - 统一使用 `PulseMarketTapeAuditEvent` / `PulseSignalSnapshotAuditEvent` / `PulseExecutionContext`
