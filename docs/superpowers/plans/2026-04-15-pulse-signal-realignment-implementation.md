# Pulse Signal Realignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `pulse_arb` 的 signal admission 从“best-ask + 静态 friction”升级成“可成交会话 EV + 真空/回补口袋”，并让 market selection 以会话 EV 为主。

**Architecture:** 在 `polyalpha-pulse` 内新增 `signal` 模块，负责 executable VWAP、vacuum/pocket 和 session edge 估计；`runtime.rs` 只消费结构化 signal 输出，继续复用现有 `EventPricer`、`PulseDetector`、`PulseSession`、`GlobalHedgeAggregator` 和执行链路。

**Tech Stack:** Rust workspace, `rust_decimal`, existing `OrderBookSnapshot` / `PulseDetector` / runtime tests, targeted `cargo test -p polyalpha-pulse`.

---

## File Structure

- Create: `crates/polyalpha-pulse/src/signal.rs`
  Responsibility: executable VWAP、vacuum/pocket、timeout proxy、session edge estimate。
- Modify: `crates/polyalpha-pulse/src/lib.rs`
  Responsibility: 导出 `signal` 模块。
- Modify: `crates/polyalpha-pulse/src/model.rs`
  Responsibility: 扩展 detector / candidate 所需的新 signal 输入输出结构。
- Modify: `crates/polyalpha-pulse/src/detector.rs`
  Responsibility: 从静态 friction 输入转为结构化 session edge 输入，补质量闸门。
- Modify: `crates/polyalpha-pulse/src/runtime.rs`
  Responsibility: 接入 `signal` 模块，改 admission 和 candidate 排序，补 audit 字段。

### Task 1: 建立 executable VWAP 和 pocket 的基础信号模块

**Files:**
- Create: `crates/polyalpha-pulse/src/signal.rs`
- Modify: `crates/polyalpha-pulse/src/lib.rs`
- Test: `crates/polyalpha-pulse/src/signal.rs`

- [ ] **Step 1: 先写失败测试，锁定盘口扫描和真空口袋语义**

在 `crates/polyalpha-pulse/src/signal.rs` 增加单元测试，至少覆盖：

```rust
#[test]
fn executable_vwap_uses_multiple_price_levels_to_fill_requested_notional() {
    // 250 USD 不能只看第一档，必须扫出真实 VWAP。
}

#[test]
fn executable_vwap_returns_shortfall_when_book_depth_is_too_thin() {
    // 深度不足时要显式给出 shortfall，不能假装 best ask 可成交。
}

#[test]
fn reversion_pocket_counts_ticks_between_entry_and_reference_price() {
    // 当前入场价到脉冲前参考价之间的 tick 数要正确。
}
```

- [ ] **Step 2: 运行测试，确认现在确实是红灯**

Run:

```bash
cargo test -p polyalpha-pulse executable_vwap_uses_multiple_price_levels_to_fill_requested_notional -- --nocapture
cargo test -p polyalpha-pulse executable_vwap_returns_shortfall_when_book_depth_is_too_thin -- --nocapture
cargo test -p polyalpha-pulse reversion_pocket_counts_ticks_between_entry_and_reference_price -- --nocapture
```

Expected:

- 测试编译失败，或因为 `signal.rs` / 对应函数不存在而失败

- [ ] **Step 3: 写最小实现，建立 signal 基础类型**

在 `crates/polyalpha-pulse/src/signal.rs` 实现最小闭环：

- `ExecutableQuote`
- `ReversionPocket`
- `SessionEdgeEstimate`
- `executable_poly_buy_quote(...)`
- `timeout_exit_sell_proxy(...)`
- `reversion_pocket(...)`

其中：

- Poly `quantity` 使用 `VenueQuantity::PolyShares`
- 买入 quote 按 asks 从低到高扫
- timeout sell proxy 按 bids 从高到低扫
- `requested_notional_usd` 明确按 `price * shares` 计算

- [ ] **Step 4: 跑 Task 1 测试，确认绿灯**

Run:

```bash
cargo test -p polyalpha-pulse executable_vwap_uses_multiple_price_levels_to_fill_requested_notional -- --nocapture
cargo test -p polyalpha-pulse executable_vwap_returns_shortfall_when_book_depth_is_too_thin -- --nocapture
cargo test -p polyalpha-pulse reversion_pocket_counts_ticks_between_entry_and_reference_price -- --nocapture
```

Expected:

- 3 个测试都 PASS

### Task 2: 把 detector 从静态 friction 输入升级成结构化 session edge

**Files:**
- Modify: `crates/polyalpha-pulse/src/model.rs`
- Modify: `crates/polyalpha-pulse/src/detector.rs`
- Test: `crates/polyalpha-pulse/src/detector.rs`

- [ ] **Step 1: 先写失败测试，锁定“有价差但会话 EV <= 0 也必须拒绝”**

在 `crates/polyalpha-pulse/src/detector.rs` 增加测试：

```rust
#[test]
fn detector_rejects_positive_anchor_gap_when_executable_session_edge_is_negative() {
    // top-of-book 看起来便宜，但吃完请求量和 timeout proxy 后 EV 为负。
}

#[test]
fn detector_rejects_when_reversion_pocket_is_too_shallow() {
    // 有 pulse，但 pocket 不够深，说明没有足够回补空间。
}

#[test]
fn detector_accepts_only_when_session_edge_and_pocket_both_pass() {
    // 只有 pulse、pocket、session EV 同时成立才放行。
}
```

- [ ] **Step 2: 运行 detector 测试，确认红灯**

Run:

```bash
cargo test -p polyalpha-pulse detector_rejects_positive_anchor_gap_when_executable_session_edge_is_negative -- --nocapture
cargo test -p polyalpha-pulse detector_rejects_when_reversion_pocket_is_too_shallow -- --nocapture
cargo test -p polyalpha-pulse detector_accepts_only_when_session_edge_and_pocket_both_pass -- --nocapture
```

Expected:

- FAIL，因为 `PulseOpportunityInput` 还没有 pocket / session EV 字段

- [ ] **Step 3: 扩模型并实现最小 admission 逻辑**

改动要求：

- `PulseOpportunityInput` 增加：
  - `expected_net_edge_bps`
  - `expected_net_pnl_usd`
  - `reversion_pocket_ticks`
  - `vacuum_ratio`
  - `pricing_quality_ok`
- `DetectorDecision` 增加：
  - `expected_net_pnl_usd`
- `PulseDetector::evaluate` 改为：
  - 先过 `anchor/data/pricing` 质量门
  - 再过 pulse 门
  - 再过 pocket / session edge 门
- 保留现有 `PulseFailureCode`，本轮尽量复用已有 rejection code，不额外扩一堆错误码

- [ ] **Step 4: 跑 detector 全量测试，确认绿灯**

Run:

```bash
cargo test -p polyalpha-pulse detector_ -- --nocapture
```

Expected:

- detector 相关测试全部 PASS

### Task 3: 把 runtime 候选生成改成 executable session EV，并按 EV 选市场

**Files:**
- Modify: `crates/polyalpha-pulse/src/runtime.rs`
- Test: `crates/polyalpha-pulse/src/runtime.rs`

- [ ] **Step 1: 先写失败测试，锁定 runtime 的新选择语义**

在 `crates/polyalpha-pulse/src/runtime.rs` 增加至少两个测试：

```rust
#[tokio::test]
async fn best_entry_candidate_prefers_higher_expected_net_pnl_over_higher_pulse_score() {
    // 一个市场 pulse 更猛但盘口更薄，另一个 pulse 较弱但 session EV 更高；必须选后者。
}

#[tokio::test]
async fn runtime_rejects_market_when_top_of_book_edge_is_positive_but_executable_session_edge_is_negative() {
    // 证明新的 admission 不是 best ask 边际幻觉。
}
```

- [ ] **Step 2: 运行 runtime 定位测试，确认红灯**

Run:

```bash
cargo test -p polyalpha-pulse best_entry_candidate_prefers_higher_expected_net_pnl_over_higher_pulse_score -- --nocapture
cargo test -p polyalpha-pulse runtime_rejects_market_when_top_of_book_edge_is_positive_but_executable_session_edge_is_negative -- --nocapture
```

Expected:

- FAIL，因为当前 runtime 仍按 `pulse_score` 选 market，且 admission 仍基于 `best_ask`

- [ ] **Step 3: 用 signal 模块重写候选生成**

实现要求：

- `EntryCandidate` 增加：
  - `expected_net_pnl_usd`
  - `entry_executable_notional_usd`
  - `timeout_exit_price`
  - `reversion_pocket_ticks`
  - `vacuum_ratio`
- `entry_candidate_for_market(...)`：
  - 用 `signal.rs` 计算 claim side executable buy quote
  - 用 timeout proxy 近似最差退出
  - 组合成 `SessionEdgeEstimate`
  - 再喂给 `detector`
- `best_entry_candidate(...)`：
  - 第一排序键改为 `expected_net_pnl_usd`
  - 第二排序键改为 `net_edge_bps`
  - 第三排序键才是 `pulse_score_bps`
- `try_open_new_session(...)`：
  - `expected_open_net_pnl_usd` 改用 candidate 的结构化结果

- [ ] **Step 4: 跑 runtime 定位测试，确认绿灯**

Run:

```bash
cargo test -p polyalpha-pulse best_entry_candidate_prefers_higher_expected_net_pnl_over_higher_pulse_score -- --nocapture
cargo test -p polyalpha-pulse runtime_rejects_market_when_top_of_book_edge_is_positive_but_executable_session_edge_is_negative -- --nocapture
```

Expected:

- 2 个测试 PASS

### Task 4: 做一轮回归验证，确认没有把执行骨架搞坏

**Files:**
- Modify: `crates/polyalpha-pulse/src/runtime.rs`（只在必要时修小口径）
- Test: `crates/polyalpha-pulse/src/runtime.rs`

- [ ] **Step 1: 运行 pulse crate 关键回归测试**

Run:

```bash
cargo test -p polyalpha-pulse runtime_tick_opens_after_confirmed_no_side_pulse -- --nocapture
cargo test -p polyalpha-pulse runtime_tick_hedges_partial_fill_between_min_and_request_notional_when_b1_gates_pass -- --nocapture
cargo test -p polyalpha-pulse runtime_timeout_cancels_maker_and_submits_poly_chasing_exit -- --nocapture
cargo test -p polyalpha-pulse runtime_tick_records_pegging_lifecycle_in_audit -- --nocapture
```

Expected:

- 关键开仓、partial fill、timeout、audit 测试都 PASS

- [ ] **Step 2: 跑 pulse crate 全量测试**

Run:

```bash
cargo test -p polyalpha-pulse -- --nocapture
```

Expected:

- 全部 PASS

- [ ] **Step 3: 人工复核与 spec 对齐**

确认以下结果已经成立：

- 代码里不再直接把 `best_ask` 当成开仓成本
- 候选排序不再由 `pulse_score` 主导
- 浅盘口假信号会因为 executable EV 为负而被拒绝
- 现有 session / hedge / exit 骨架没有被顺手改坏
