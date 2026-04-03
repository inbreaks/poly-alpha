# CEX Orderbook Canonical Storage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove duplicate CEX snapshot storage in `InMemoryOrderbookProvider` while keeping symbol-keyed and venue-keyed reads working.

**Architecture:** Store CEX snapshots canonically by `(exchange, venue_symbol)` and add a lightweight symbol-to-venue index for symbol-keyed CEX reads. Keep non-CEX snapshots in the existing symbol-keyed map.

**Tech Stack:** Rust, Cargo tests, existing `polyalpha-executor` provider interfaces

---

### Task 1: Lock In Expected CEX Alias Behavior

**Files:**
- Modify: `crates/polyalpha-executor/src/orderbook_provider.rs`
- Test: `crates/polyalpha-executor/src/orderbook_provider.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn cex_symbol_lookup_reads_latest_shared_venue_snapshot() {
    let provider = InMemoryOrderbookProvider::new();
    provider.update_cex(sample_cex_snapshot("market-a", 1), "BTCUSDT".to_owned());
    provider.update_cex(sample_cex_snapshot("market-b", 2), "BTCUSDT".to_owned());

    let book = provider
        .get_orderbook(
            Exchange::Binance,
            &Symbol::new("market-a"),
            InstrumentKind::CexPerp,
        )
        .expect("symbol lookup should resolve through venue alias");

    assert_eq!(book.sequence, 2);
    assert_eq!(book.symbol, Symbol::new("market-a"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p polyalpha-executor cex_symbol_lookup_reads_latest_shared_venue_snapshot -- --exact`
Expected: FAIL because symbol-keyed CEX lookup still returns the stale per-symbol snapshot.

### Task 2: Implement Canonical CEX Storage

**Files:**
- Modify: `crates/polyalpha-executor/src/orderbook_provider.rs`
- Test: `crates/polyalpha-executor/src/orderbook_provider.rs`

- [ ] **Step 1: Add canonical CEX storage structures**

```rust
cex_orderbooks: Arc<RwLock<HashMap<(Exchange, String), OrderBookSnapshot>>>,
cex_symbol_index: Arc<RwLock<HashMap<(Exchange, Symbol), String>>>,
```

- [ ] **Step 2: Update CEX write path**

```rust
pub fn update_cex(&self, snapshot: OrderBookSnapshot, venue_symbol: String) {
    if let Ok(mut cex_map) = self.cex_orderbooks.write() {
        cex_map.insert((snapshot.exchange, venue_symbol.clone()), snapshot.clone());
    }

    if let Ok(mut symbol_index) = self.cex_symbol_index.write() {
        symbol_index.insert((snapshot.exchange, snapshot.symbol.clone()), venue_symbol);
    }
}
```

- [ ] **Step 3: Route symbol-keyed CEX reads through the canonical venue snapshot**

```rust
if instrument == InstrumentKind::CexPerp {
    let venue_symbol = {
        let index = self.cex_symbol_index.read().ok()?;
        index.get(&(exchange, symbol.clone())).cloned()?
    };
    let mut snapshot = {
        let books = self.cex_orderbooks.read().ok()?;
        books.get(&(exchange, venue_symbol)).cloned()?
    };
    snapshot.symbol = symbol.clone();
    return Some(snapshot);
}
```

- [ ] **Step 4: Run the focused test to verify it passes**

Run: `cargo test -p polyalpha-executor cex_symbol_lookup_reads_latest_shared_venue_snapshot -- --exact`
Expected: PASS

### Task 3: Run Broader Verification

**Files:**
- Modify: `crates/polyalpha-executor/src/orderbook_provider.rs`
- Test: `crates/polyalpha-executor/src/orderbook_provider.rs`

- [ ] **Step 1: Run executor tests**

Run: `cargo test -p polyalpha-executor`
Expected: PASS with 0 failures

- [ ] **Step 2: Run the CLI build that exercises live mock**

Run: `cargo build -p polyalpha-cli`
Expected: PASS

- [ ] **Step 3: Reprofile the 256-market live mock scenario**

Run: `./target/debug/polyalpha-cli live run-multi --env multi-market-active.fresh --executor-mode mock --max-ticks 0 --poll-interval-ms 5000 --print-every 1000 --warmup-klines 0`
Expected: steady-state CPU remains well below the old `50%+` regression, with the CEX orderbook clone path reduced in the profiler.
