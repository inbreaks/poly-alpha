# CEX Orderbook Canonical Storage Design

## Goal

Reduce live mock CPU in the CEX orderbook update path by removing duplicate full-snapshot storage while preserving existing read APIs.

## Problem

`apply_orderbook_snapshot()` currently clones each CEX `OrderBookSnapshot` and passes it into `InMemoryOrderbookProvider::update_cex()`. `update_cex()` then clones the same snapshot again so it can store one copy in `orderbooks` and another in `cex_orderbooks`.

This causes two avoidable costs on the hot path:

1. Two full `OrderBookSnapshot` clone/drop cycles per CEX update.
2. Two full in-memory copies of the same venue snapshot.

It also leaves `get_orderbook(exchange, symbol, CexPerp)` with symbol-local state, so two markets sharing the same `venue_symbol` can observe different CEX books even though downstream CEX execution is venue-keyed.

## Design

Treat CEX orderbooks as canonical by `(exchange, venue_symbol)`.

- Keep `orderbooks` for symbol-keyed non-CEX snapshots.
- Keep `cex_orderbooks` for canonical CEX snapshots keyed by `(exchange, venue_symbol)`.
- Add a lightweight `cex_symbol_index` keyed by `(exchange, symbol)` that stores the corresponding `venue_symbol`.

`update_cex()` will:

1. Insert the latest snapshot once into `cex_orderbooks`.
2. Update the symbol-to-venue index in `cex_symbol_index`.

`get_cex_orderbook()` will continue reading directly from `cex_orderbooks`.

`get_orderbook(..., CexPerp)` will:

1. Resolve `(exchange, symbol)` to a `venue_symbol` through `cex_symbol_index`.
2. Read the canonical snapshot from `cex_orderbooks`.
3. Return a cloned snapshot whose `symbol` field is rewritten to the requested symbol.

That preserves the existing symbol-keyed API while ensuring all symbols sharing a venue read the same latest venue book.

## Scope

In scope:

- `crates/polyalpha-executor/src/orderbook_provider.rs`
- Provider unit tests for CEX alias behavior

Out of scope:

- Monitor publish optimizations
- Audit writer batching
- Broader orderbook/provider interface redesign

## Verification

- Add a failing unit test showing two symbols on the same venue share the latest canonical CEX snapshot.
- Run the focused provider test.
- Run `cargo test -p polyalpha-executor`.
- Rebuild and rerun the 256-market live mock profile to confirm the orderbook path gets cheaper without behavior regressions.
