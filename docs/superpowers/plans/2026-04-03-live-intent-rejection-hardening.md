# Live Intent Rejection Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep live/mock-live runtime running when planner rejects close or rebalance intents, and always finalize audit state on early exit.

**Architecture:** Treat intent-side planner rejection as a normal runtime outcome instead of a process-level error. Route that outcome through the same Chinese rejection/audit surface used elsewhere, then wrap live runtime exits so audit summary always transitions out of `running`.

**Tech Stack:** Rust, Tokio, `polyalpha-cli`, `polyalpha-executor`, `polyalpha-audit`

---

### Task 1: Lock Regression With Failing Tests

**Files:**
- Modify: `crates/polyalpha-cli/src/paper.rs`
- Test: `crates/polyalpha-cli/src/paper.rs`

- [ ] **Step 1: Write the failing test for non-fatal intent rejection**

```rust
#[tokio::test]
async fn execute_intent_trigger_converts_plan_rejection_into_runtime_event() {
    let mut stats = PaperStats::default();
    let mut recent_events = VecDeque::new();
    let mut trade_book = TradeBook::default();
    let settings = test_settings_with_price_filter(None, None);
    let registry = SymbolRegistry::new(settings.markets.clone());
    let provider = Arc::new(InMemoryOrderbookProvider::new());
    let mut execution = ExecutionManager::with_symbol_registry_and_orderbook_provider(
        RuntimeExecutor::DryRun(DryRunExecutor::default()),
        registry,
        provider,
    );
    let mut engine = SimpleAlphaEngine::with_markets(
        build_paper_engine_config(&settings),
        settings.markets.clone(),
    );
    let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));

    let result = execute_intent_trigger(
        close_intent_for_symbol(&settings.markets[0].symbol, "test-close", "corr-test", 1_000),
        1_000,
        &mut engine,
        &mut execution,
        &mut risk,
        &mut stats,
        &mut recent_events,
        &mut trade_book,
    )
    .await;

    assert!(result.is_ok());
    assert!(recent_events.iter().any(|event| event.summary.contains("insufficient_poly_depth")));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `CARGO_NET_OFFLINE=true cargo test -p polyalpha-cli execute_intent_trigger_converts_plan_rejection_into_runtime_event -- --nocapture`
Expected: FAIL because `execute_intent_trigger` currently bubbles `plan rejected [insufficient_poly_depth]`.

- [ ] **Step 3: Write the failing test for audit finalization on early runtime exit**

```rust
#[test]
fn finalize_live_audit_after_runtime_error_marks_summary_completed() {
    let data_dir = unique_test_data_dir("live-intent-finalize-on-error");
    let settings = test_settings_with_audit_data_dir(data_dir.clone());
    let mut audit = PaperAudit::start(
        &settings,
        "test-env",
        &settings.markets,
        1_000,
        polyalpha_core::TradingMode::Live,
    )
    .expect("start live audit");
    let session_id = audit.session_id().to_owned();

    finalize_audit_on_runtime_exit(
        Some(&mut audit),
        &PaperStats::default(),
        &MonitorState::default(),
        3,
        5_000,
    )
    .expect("finalize audit");

    let summary = polyalpha_audit::AuditReader::load_summary(&data_dir, &session_id)
        .expect("load summary");
    assert_eq!(summary.status, AuditSessionStatus::Completed);
    assert_eq!(summary.ended_at_ms, Some(5_000));
}
```

- [ ] **Step 4: Run test to verify it fails**

Run: `CARGO_NET_OFFLINE=true cargo test -p polyalpha-cli finalize_live_audit_after_runtime_error_marks_summary_completed -- --nocapture`
Expected: FAIL because no shared early-exit finalize helper exists yet.


### Task 2: Downgrade Intent Rejections Into Runtime Events

**Files:**
- Modify: `crates/polyalpha-cli/src/paper.rs`
- Test: `crates/polyalpha-cli/src/paper.rs`

- [ ] **Step 1: Implement minimal helper that classifies intent-side planner rejection**

```rust
fn intent_rejection_reason_code(err: &anyhow::Error) -> Option<String> {
    extract_plan_rejection_reason(&err.to_string())
}
```

- [ ] **Step 2: Update `execute_intent_trigger` to consume normal planner rejection**

```rust
let events = match execution
    .process_intent(trigger.intent().cloned().expect("intent trigger"))
    .await
{
    Ok(events) => events,
    Err(err) if intent_rejection_reason_code(&err).is_some() => {
        track_intent_execution_rejection(...)?;
        sync_engine_position_state_from_runtime(...);
        return Ok(());
    }
    Err(err) => return Err(err),
};
```

- [ ] **Step 3: Record the rejection into recent events and audit**

```rust
push_event_with_context_and_payload(
    recent_events,
    "risk",
    format!("{} 本轮未执行（{}）", symbol.0, gate_reason_label_zh("trade_plan", &reason_code)),
    Some(symbol.0.clone()),
    None,
    Some(trigger.correlation_id().to_owned()),
    Some(json!({ "reason_code": reason_code, "intent_type": trigger.kind_code() })),
);
```

- [ ] **Step 4: Run the focused tests**

Run: `CARGO_NET_OFFLINE=true cargo test -p polyalpha-cli execute_intent_trigger_converts_plan_rejection_into_runtime_event -- --nocapture`
Expected: PASS, with runtime returning `Ok(())` and a visible rejection event.


### Task 3: Guarantee Audit Finalization On Early Exit

**Files:**
- Modify: `crates/polyalpha-cli/src/paper.rs`
- Test: `crates/polyalpha-cli/src/paper.rs`

- [ ] **Step 1: Extract shared audit-finalize helper**

```rust
fn finalize_audit_on_runtime_exit(
    audit: Option<&mut PaperAudit>,
    stats: &PaperStats,
    monitor_state: &MonitorState,
    tick_index: usize,
    now_ms: u64,
) -> Result<()> {
    if let Some(audit) = audit {
        audit.finalize(stats, monitor_state, tick_index, now_ms)?;
    }
    Ok(())
}
```

- [ ] **Step 2: Wrap live/paper multi-market tail so finalize still runs after loop errors**

```rust
let loop_result: Result<()> = async {
    loop {
        // existing body
    }
    Ok(())
}
.await;

let finalize_result = finalize_audit_on_runtime_exit(...);
loop_result?;
finalize_result?;
```

- [ ] **Step 3: Run the focused finalize test**

Run: `CARGO_NET_OFFLINE=true cargo test -p polyalpha-cli finalize_live_audit_after_runtime_error_marks_summary_completed -- --nocapture`
Expected: PASS, with audit summary leaving `running`.

- [ ] **Step 4: Run the combined regression slice**

Run: `CARGO_NET_OFFLINE=true cargo test -p polyalpha-cli execute_intent_trigger_converts_plan_rejection_into_runtime_event finalize_live_audit_after_runtime_error_marks_summary_completed paper_audit_finalize_warehouse_sync_error_is_non_fatal -- --nocapture`
Expected: PASS for all selected tests.

