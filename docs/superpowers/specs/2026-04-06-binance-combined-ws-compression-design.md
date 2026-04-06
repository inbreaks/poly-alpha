# Binance Combined WS Compression Design

## Goal

Reduce Binance websocket transport overhead in multi-market WS mode by replacing per-symbol Binance sockets with a single combined stream, while preserving current behavior through an explicit config gate and automatic fallback to the existing per-symbol path.

## Problem

In the current WS runtime, Polymarket already uses one socket with many `asset_id` subscriptions, but Binance still opens one websocket per unique `venue_symbol`.

That creates avoidable transport and connection-management overhead:

1. More Binance websocket handshakes and reconnect loops than necessary.
2. More socket-level heartbeat / read / reconnect work in the runtime hot path.
3. No explicit attempt to negotiate websocket compression even though the upstream Binance endpoint supports `permessage-deflate`.

At the same time, this path is operationally sensitive, so we cannot replace the current per-symbol mode with a riskier transport shape without a controlled rollout and a safe fallback.

## Constraints

- This change only targets Binance websocket transport.
- OKX and Polymarket behavior stay unchanged.
- The feature must be explicitly gated in config and default to off.
- Failure of the new combined path must not take Binance market data down; it must fall back to the current per-symbol path in the same runtime.
- Failure to negotiate compression is not itself a fatal error. Combined mode may still be useful even without compression.

## Approaches

1. Keep the current per-symbol Binance sockets and only add websocket compression negotiation.
   Low implementation risk, but it does not reduce connection count or reconnect overhead.

2. Replace per-symbol Binance sockets with a single combined socket, explicitly request `permessage-deflate`, and fall back to the current per-symbol mode if the combined path fails. (Recommended)
   This directly targets connection overhead while keeping rollout risk controlled.

3. Build a sharded combined-stream transport from the start.
   More scalable long term, but heavier than needed for the first validation pass and adds unnecessary rollout complexity.

## Design

### Config

Add a new field under `strategy.market_data`:

```toml
[strategy.market_data]
binance_combined_ws_enabled = true
```

Rules:

- Default value is `false`.
- Existing configs without the field must continue to deserialize unchanged.
- The field only controls Binance websocket transport selection in WS runtime.

This field belongs in `strategy.market_data`, not `binance`, because it changes runtime transport behavior rather than exchange endpoint identity.

### Runtime transport selection

When `binance_combined_ws_enabled = false`, keep the current behavior unchanged:

- collect unique Binance `venue_symbol`s
- spawn one Binance websocket task per symbol

When `binance_combined_ws_enabled = true`:

- collect the same unique Binance `venue_symbol`s
- build one Binance combined-stream endpoint containing all of them
- spawn a single Binance combined websocket task instead of per-symbol tasks

The combined path is only an alternative transport shape. It does not change downstream orderbook normalization or signal evaluation logic.

### Binance combined websocket task

Add a dedicated runtime task for the combined Binance transport.

Responsibilities:

1. Build the combined endpoint from all selected Binance streams.
2. Connect with an explicit websocket handshake that requests `permessage-deflate`.
3. Log whether compression was negotiated.
4. Read combined payloads and route them through the existing Binance depth parser.
5. On combined-path failure, fall back to the existing per-symbol spawn logic.

This task should reuse the same reconnect backoff policy and tracker model used by the current per-symbol path.

### Compression negotiation

The Binance combined task should send a custom websocket handshake that explicitly requests:

- `Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits`

Behavior:

- If Binance responds with `Sec-WebSocket-Extensions`, log the negotiated value.
- If Binance upgrades successfully but does not negotiate compression, log that result and continue in combined mode.
- Only handshake failure or runtime transport failure triggers fallback.

This keeps compression as an opportunistic optimization instead of turning it into a rollout blocker.

### Parsing and message handling

No new Binance business parser should be introduced.

The current parser already accepts both:

- raw depth payloads
- combined payloads with a top-level `data` wrapper

That parser should remain the canonical decode path for both old and new Binance transports.

### Connection tracking semantics

The current connection tracker is keyed by a `stream_key`. In per-symbol mode that key is the Binance `venue_symbol`.

For the combined path:

- mark every Binance `venue_symbol` in the combined set as connected once the combined socket is established
- mark the same set as disconnected if the combined socket ends
- keep per-symbol freshness behavior driven by actual quote timestamps, not by socket-level liveness

This preserves the current monitoring semantics:

- socket connectivity can still be shown as healthy
- individual symbols can still become stale if they stop receiving updates

### Automatic fallback

Fallback target: the current per-symbol Binance websocket implementation.

Fallback should trigger on combined-path transport failure, including:

- connect failure
- websocket handshake failure
- combined-stream read error
- close frame / unexpected stream end
- startup path failure before the combined socket yields its first successfully parsed Binance depth update

Fallback should not trigger merely because compression was not negotiated.

Fallback behavior:

- log one clear warning that combined mode failed and the runtime is reverting to per-symbol Binance sockets
- start the existing per-symbol Binance websocket tasks
- do not oscillate back into combined mode during the same runtime session
- perform the fallback at most once per runtime session

This makes the feature safe for opt-in validation in `multi-market-active.fresh`.

## Scope

In scope:

- `crates/polyalpha-core/src/config.rs`
- `config/*.toml` for opt-in env wiring
- `crates/polyalpha-cli/src/paper.rs`
- focused runtime / config tests covering the new Binance transport selection and fallback behavior

Out of scope:

- OKX transport aggregation
- Polymarket transport changes
- Binance transport sharding
- general websocket transport abstraction refactors

## Verification

### Config verification

- Add / update config tests to prove `binance_combined_ws_enabled` defaults to `false`.
- Add / update config tests to prove the field deserializes from `strategy.market_data`.
- Run `check-config` against the opt-in environment that enables the feature.

### Runtime verification

- Add a focused runtime test for transport selection:
  when the flag is off, runtime stays on per-symbol Binance sockets;
  when the flag is on, runtime selects the combined Binance path.
- Add a focused runtime test for fallback:
  if the combined Binance connect path fails, the runtime starts the current per-symbol Binance sockets instead.
- Add a focused unit test around handshake / response handling that records whether compression was negotiated.

### Live validation

Use the same market pool and compare two WS mock runs:

1. `binance_combined_ws_enabled = false`
2. `binance_combined_ws_enabled = true`

Compare:

- Binance connection logs
- whether compression was negotiated
- `cex_quote_stale`
- `connection_lost`
- `signals_seen`
- `market_events`

The expected success criterion for the first rollout is:

- no Binance data loss regression
- no runtime crash or persistent disconnect regression
- fewer Binance websocket connections in logs
- optional improvement in CEX-side staleness / stability

## Risks

- Binance combined streams may behave differently under reconnect churn than raw per-symbol sockets.
- Combined mode concentrates all Binance traffic into one socket, so fallback behavior must be clean and immediate.
- Compression support may vary by upstream edge or deployment path, so missing compression negotiation must not be treated as fatal.

## Next step after approval

Write an implementation plan for the minimum viable rollout:

1. config field and tests
2. explicit Binance websocket handshake with compression request
3. combined Binance runtime task
4. one-shot fallback to current per-symbol mode
5. opt-in env verification in `multi-market-active.fresh`
