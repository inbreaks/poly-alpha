#!/usr/bin/env python3
"""
构建 BTC 基差套利回测数据库。

默认时间范围:
- start_date: 结束日往前 90 天
- end_date:   昨日 UTC

运行示例:
    python3 scripts/build_btc_basis_backtest_db.py

依赖安装:
    python3 -m pip install -r requirements/backtest-db.txt
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import math
import os
import re
import shutil
import socket
import ssl
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Iterable
from urllib.error import HTTPError, URLError

try:
    import ccxt  # type: ignore
    import duckdb  # type: ignore
except ModuleNotFoundError as exc:  # pragma: no cover
    missing = exc.name or "dependency"
    raise SystemExit(
        f"missing Python dependency `{missing}`; run "
        "`python3 -m pip install -r requirements/backtest-db.txt` first"
    ) from exc


GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
GAMMA_PUBLIC_SEARCH_URL = "https://gamma-api.polymarket.com/public-search"
CLOB_PRICE_HISTORY_URL = "https://clob.polymarket.com/prices-history"
BINANCE_SPOT_KLINES_URL = "https://data-api.binance.vision/api/v3/klines"
DEFAULT_OUTPUT = "data/btc_basis_backtest_price_only_ready.duckdb"
DEFAULT_MARKET_STYLE = "price-only"
BTC_KEYWORDS = ("btc", "bitcoin")
PRICE_SEARCH_QUERIES = (
    "bitcoin",
    "btc",
    "100k",
    "90k",
    "80k",
    "120k",
    "all-time high",
)
BTC_CONTEXT_PATTERNS = (
    r"\bprice\b",
    r"\breach\b",
    r"\bhit\b",
    r"\babove\b",
    r"\bbelow\b",
    r"\b100k\b",
    r"\b90k\b",
    r"\b80k\b",
    r"\b120k\b",
    r"\breserve\b",
    r"\bpurchase\b",
    r"\bbuy\b",
    r"\bsell\b",
    r"\btreasury\b",
    r"\betf\b",
    r"\bhalving\b",
    r"\bath\b",
    r"\ball-time high\b",
    r"\bclose above\b",
    r"\bclose below\b",
    r"\bbitcoin hit\b",
    r"\bbitcoin reach\b",
    r"\bbtc hit\b",
    r"\bbtc reach\b",
)
BTC_FALSE_POSITIVE_HINTS = (
    "btc gaming",
    "dota 2",
    "liquipedia.net",
    "game 1 winner",
    "game 2 winner",
    "game 3 winner",
    "bo3",
    "bo5",
    "kill handicap",
)
BTC_PRICE_CONTEXT_PATTERNS = (
    r"\bbitcoin price\b",
    r"\bbtc price\b",
    r"\bprice on\b",
    r"\bwhat price will bitcoin hit\b",
    r"\bwhat price will btc hit\b",
    r"\bbitcoin above\b",
    r"\bbtc above\b",
    r"\bbitcoin below\b",
    r"\bbtc below\b",
    r"\bwill bitcoin hit\b",
    r"\bwill btc hit\b",
    r"\bbitcoin close above\b",
    r"\bbtc close above\b",
    r"\bbitcoin close below\b",
    r"\bbtc close below\b",
    r"\bbitcoin ath\b",
    r"\bbtc ath\b",
    r"\ball-time high\b",
)
BTC_PRICE_FALSE_POSITIVE_HINTS = (
    "microstrategy",
    "announce",
    "announces",
    "holding",
    "purchase",
    "reserve act",
    "strategic reserve",
    "treasury",
    "etf",
    "silver",
    "gold",
    "first?",
    " first ",
    "up or down",
)
EVENT_PAGE_LIMIT = 100
SEARCH_PAGE_LIMIT = 100
PRICE_SEARCH_MAX_PAGES = 20
PRICE_SEARCH_MAX_EMPTY_PAGES = 3
BINANCE_PAGE_LIMIT = 1500
BINANCE_SPOT_PAGE_LIMIT = 1000
API_MAX_RETRIES = 5
BINANCE_MAX_RETRIES = 8
BINANCE_TIMEOUT_MS = 30_000
RETRYABLE_HTTP_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
DEFAULT_POLY_HISTORY_WORKERS = 8
DUCKDB_INSERT_BATCH_SIZE = 100_000
DUCKDB_COPY_PROGRESS_EVERY = 100_000


@dataclass
class ContractToken:
    event_id: str
    event_title: str
    event_slug: str
    market_id: str
    market_question: str
    condition_id: str | None
    token_id: str
    token_side: str
    start_ts: int
    end_ts: int


def parse_args() -> argparse.Namespace:
    default_end = datetime.now(UTC).date() - timedelta(days=1)
    default_start = default_end - timedelta(days=90)

    parser = argparse.ArgumentParser(description="Build BTC basis backtest DuckDB")
    parser.add_argument(
        "--start-date",
        default=default_start.isoformat(),
        help="UTC date, YYYY-MM-DD; default is 90 days before end-date",
    )
    parser.add_argument(
        "--end-date",
        default=default_end.isoformat(),
        help="UTC date, YYYY-MM-DD; default is yesterday UTC",
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="DuckDB output path")
    parser.add_argument(
        "--market-style",
        choices=("all-btc", "price-only"),
        default=DEFAULT_MARKET_STYLE,
        help="Sample scope: `price-only` keeps only BTC price markets; `all-btc` keeps broader BTC-related events",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Optional cap on discovered BTC events for debugging",
    )
    parser.add_argument(
        "--max-contracts",
        type=int,
        default=0,
        help="Optional cap on token contracts for debugging",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.1,
        help="Small sleep between Polymarket history requests",
    )
    parser.add_argument(
        "--poly-history-workers",
        type=int,
        default=DEFAULT_POLY_HISTORY_WORKERS,
        help="Concurrent worker count for Polymarket history fetching",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start_dt = parse_date_utc(args.start_date)
    end_dt = parse_date_utc(args.end_date)
    end_exclusive = datetime(
        end_dt.year, end_dt.month, end_dt.day, 23, 59, 59, tzinfo=UTC
    )

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)

    print(
        f"discovering BTC events between {start_dt.isoformat()} and {end_exclusive.isoformat()} "
        f"(market_style={args.market_style})"
    )
    events = fetch_btc_events(start_dt, end_exclusive, args.max_events, args.market_style)
    contracts = extract_contract_tokens(events, start_dt, end_exclusive, args.max_contracts)
    if not contracts:
        raise SystemExit(
            f"no `{args.market_style}` BTC settled contracts found in the requested range"
        )

    global_start = min(row.start_ts for row in contracts)
    global_end = max(row.end_ts for row in contracts)

    print(f"found {len(events)} BTC events and {len(contracts)} token contracts")
    poly_rows, poly_failures = fetch_all_poly_histories(
        contracts,
        sleep_seconds=args.sleep_seconds,
        max_workers=args.poly_history_workers,
    )
    print(f"fetched {len(poly_rows)} polymarket price rows")
    if poly_failures:
        print(f"skipped {len(poly_failures)} polymarket contracts without usable price history")

    kline_rows, cex_source = fetch_binance_klines(global_start, global_end)
    print(f"fetched {len(kline_rows)} binance 1m rows from {cex_source}")

    basis_rows = build_basis_rows(poly_rows, kline_rows)
    print(f"built {len(basis_rows)} aligned basis rows")

    write_duckdb(
        args.output,
        events,
        contracts,
        poly_rows,
        poly_failures,
        cex_source,
        kline_rows,
        basis_rows,
        args.market_style,
    )
    print(f"duckdb written to {args.output}")


def parse_date_utc(raw: str) -> datetime:
    return datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=UTC)


def parse_dt(raw: str | None) -> datetime | None:
    if not raw:
        return None

    text = raw.strip()
    if not text:
        return None

    # Gamma 有时是 ISO8601，有时是 `YYYY-MM-DD HH:MM:SS+00`。
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if " " in text and "T" not in text:
        left, right = text.split(" ", 1)
        text = f"{left}T{right}"

    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def http_get_json(url: str, params: dict[str, Any]) -> Any:
    query = urllib.parse.urlencode(params, doseq=True)
    full_url = f"{url}?{query}" if query else url
    request = urllib.request.Request(
        full_url,
        headers={
            "User-Agent": "polyalpha-backtest-builder/0.1",
            "Accept": "application/json",
        },
    )
    last_error: Exception | None = None

    for attempt in range(API_MAX_RETRIES):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = response.read().decode("utf-8")
            return json.loads(payload)
        except HTTPError as exc:
            if exc.code not in RETRYABLE_HTTP_STATUS_CODES or attempt + 1 >= API_MAX_RETRIES:
                raise
            last_error = exc
        except (
            URLError,
            TimeoutError,
            ConnectionError,
            socket.timeout,
            ssl.SSLError,
            json.JSONDecodeError,
        ) as exc:
            if attempt + 1 >= API_MAX_RETRIES:
                raise
            last_error = exc

        # 这里主要兜住偶发的 EOF / 429 / 5xx，不改变正常逻辑。
        sleep_seconds = retry_sleep_seconds(attempt)
        print(
            f"retrying request after {type(last_error).__name__}: {full_url} "
            f"(sleep={sleep_seconds:.1f}s)",
            file=sys.stderr,
            flush=True,
        )
        time.sleep(sleep_seconds)

    raise RuntimeError(f"unreachable http_get_json retry loop for {full_url}")


def fetch_btc_events(
    start_dt: datetime,
    end_dt: datetime,
    max_events: int = 0,
    market_style: str = DEFAULT_MARKET_STYLE,
) -> list[dict[str, Any]]:
    searched_events = fetch_btc_events_via_public_search(start_dt, end_dt, max_events, market_style)
    if searched_events:
        return searched_events

    return fetch_btc_events_via_event_scan(start_dt, end_dt, max_events, market_style)


def fetch_btc_events_via_public_search(
    start_dt: datetime,
    end_dt: datetime,
    max_events: int = 0,
    market_style: str = DEFAULT_MARKET_STYLE,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen_event_ids: set[str] = set()

    queries = PRICE_SEARCH_QUERIES if market_style == "price-only" else ("btc",)
    for query in queries:
        page = 1
        empty_pages = 0
        while True:
            if market_style == "price-only" and page > PRICE_SEARCH_MAX_PAGES:
                break

            payload = http_get_json(
                GAMMA_PUBLIC_SEARCH_URL,
                {
                    "q": query,
                    "events_status": "closed",
                    "limit_per_type": SEARCH_PAGE_LIMIT,
                    "page": page,
                    "search_tags": "false",
                    "search_profiles": "false",
                },
            )
            if not isinstance(payload, dict):
                break

            events = payload.get("events", []) or []
            if not isinstance(events, list) or not events:
                break

            page_new_hits = 0
            for event in events:
                if not isinstance(event, dict):
                    continue

                closed_dt = resolve_event_closed_dt(event)
                if closed_dt is not None and (closed_dt < start_dt or closed_dt > end_dt):
                    continue
                if not event_matches_market_style(event, market_style):
                    continue

                event_id = str(event.get("id", "")).strip()
                if not event_id or event_id in seen_event_ids:
                    continue

                seen_event_ids.add(event_id)
                out.append(event)
                page_new_hits += 1
                if max_events and len(out) >= max_events:
                    return out

            if market_style == "price-only":
                if page_new_hits == 0:
                    empty_pages += 1
                else:
                    empty_pages = 0
                if empty_pages >= PRICE_SEARCH_MAX_EMPTY_PAGES:
                    break

            pagination = payload.get("pagination") or {}
            if not pagination.get("hasMore"):
                break
            page += 1

    out.sort(key=sort_event_closed_time, reverse=True)
    return out


def fetch_btc_events_via_event_scan(
    start_dt: datetime,
    end_dt: datetime,
    max_events: int = 0,
    market_style: str = DEFAULT_MARKET_STYLE,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen_event_ids: set[str] = set()
    offset = 0

    while True:
        page = http_get_json(
            GAMMA_EVENTS_URL,
            {
                "active": "false",
                "closed": "true",
                "limit": EVENT_PAGE_LIMIT,
                "offset": offset,
                "order": "closedTime",
                "ascending": "false",
                "end_date_min": start_dt.isoformat().replace("+00:00", "Z"),
                "end_date_max": end_dt.isoformat().replace("+00:00", "Z"),
            },
        )
        if not isinstance(page, list) or not page:
            break

        should_stop = False
        for event in page:
            closed_dt = resolve_event_closed_dt(event)
            if closed_dt is not None:
                if closed_dt < start_dt:
                    should_stop = True
                    continue
                if closed_dt > end_dt:
                    continue

            if event_matches_market_style(event, market_style):
                event_id = str(event.get("id", "")).strip()
                if event_id in seen_event_ids:
                    continue
                seen_event_ids.add(event_id)
                out.append(event)
                if max_events and len(out) >= max_events:
                    return out

        offset += len(page)
        if should_stop or len(page) < EVENT_PAGE_LIMIT:
            break

    return out


def is_btc_event(event: dict[str, Any]) -> bool:
    fields = [
        str(event.get("title", "")),
        str(event.get("slug", "")),
        str(event.get("ticker", "")),
        str(event.get("description", "")),
    ]
    for market in event.get("markets", []) or []:
        fields.append(str(market.get("question", "")))
        fields.append(str(market.get("slug", "")))
        fields.append(str(market.get("description", "")))

    haystack = " ".join(fields).lower()
    if "bitcoin" in haystack:
        return True

    if not re.search(r"(?<![a-z0-9])btc(?![a-z0-9])", haystack):
        return False

    # 这里要排掉 `BTC Gaming` 这类队名误命中，只保留更像比特币行情/事件的问题。
    has_context = any(re.search(pattern, haystack) for pattern in BTC_CONTEXT_PATTERNS)
    has_false_positive_hint = any(keyword in haystack for keyword in BTC_FALSE_POSITIVE_HINTS)
    if has_false_positive_hint and not has_context:
        return False

    return has_context


def event_text_haystack(event: dict[str, Any]) -> str:
    fields = [
        str(event.get("title", "")),
        str(event.get("slug", "")),
        str(event.get("ticker", "")),
        str(event.get("description", "")),
    ]
    for market in event.get("markets", []) or []:
        fields.append(str(market.get("question", "")))
        fields.append(str(market.get("slug", "")))
        fields.append(str(market.get("description", "")))
    return " ".join(fields).lower()


def is_btc_price_event(event: dict[str, Any]) -> bool:
    haystack = event_text_haystack(event)
    if not is_btc_event(event):
        return False

    has_price_context = any(re.search(pattern, haystack) for pattern in BTC_PRICE_CONTEXT_PATTERNS)
    has_false_positive_hint = any(keyword in haystack for keyword in BTC_PRICE_FALSE_POSITIVE_HINTS)
    return has_price_context and not has_false_positive_hint


def event_matches_market_style(event: dict[str, Any], market_style: str) -> bool:
    if market_style == "price-only":
        return is_btc_price_event(event)
    return is_btc_event(event)


def extract_contract_tokens(
    events: list[dict[str, Any]],
    global_start: datetime,
    global_end: datetime,
    max_contracts: int = 0,
) -> list[ContractToken]:
    rows: list[ContractToken] = []

    for event in events:
        event_id = str(event.get("id", ""))
        event_title = str(event.get("title", ""))
        event_slug = str(event.get("slug", ""))

        for market in event.get("markets", []) or []:
            token_ids = parse_token_ids(market.get("clobTokenIds"))
            if not token_ids:
                continue

            market_start = first_non_none_dt(
                parse_dt(market.get("startDate")),
                parse_dt(event.get("startDate")),
                parse_dt(event.get("creationDate")),
                global_start,
            )
            market_end = first_non_none_dt(
                parse_dt(market.get("closedTime")),
                parse_dt(market.get("endDate")),
                parse_dt(event.get("closedTime")),
                parse_dt(event.get("endDate")),
                global_end,
            )

            start_ts = max(int(market_start.timestamp()), int(global_start.timestamp()))
            end_ts = min(int(market_end.timestamp()), int(global_end.timestamp()))
            if start_ts >= end_ts:
                continue

            outcomes = parse_string_array(market.get("outcomes"))
            sides = [("yes", 0), ("no", 1)]
            for side_name, idx in sides:
                if idx >= len(token_ids):
                    continue
                token_id = token_ids[idx]
                label = outcomes[idx] if idx < len(outcomes) else side_name.upper()
                rows.append(
                    ContractToken(
                        event_id=event_id,
                        event_title=event_title,
                        event_slug=event_slug,
                        market_id=str(market.get("id", "")),
                        market_question=str(market.get("question", label)),
                        condition_id=market.get("conditionId"),
                        token_id=str(token_id),
                        token_side=side_name,
                        start_ts=start_ts,
                        end_ts=end_ts,
                    )
                )
                if max_contracts and len(rows) >= max_contracts:
                    return rows

    return rows


def parse_token_ids(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(item) for item in raw if str(item).strip()]
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return [text]
        if isinstance(parsed, list):
            return [str(item) for item in parsed if str(item).strip()]
        return [str(parsed)]
    return [str(raw)]


def parse_string_array(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(item) for item in raw]
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return [text]
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
        return [str(parsed)]
    return [str(raw)]


def first_non_none_dt(*values: datetime | None) -> datetime:
    for value in values:
        if value is not None:
            return value
    raise ValueError("expected at least one datetime")


def resolve_event_closed_dt(event: dict[str, Any]) -> datetime | None:
    return parse_dt(event.get("closedTime")) or parse_dt(event.get("endDate"))


def retry_sleep_seconds(attempt: int) -> float:
    return min(8.0, 1.0 * (2**attempt))


def sort_event_closed_time(event: dict[str, Any]) -> float:
    closed_dt = resolve_event_closed_dt(event)
    if closed_dt is None:
        return 0.0
    return closed_dt.timestamp()


def fetch_all_poly_histories(
    contracts: Iterable[ContractToken],
    sleep_seconds: float,
    max_workers: int,
) -> tuple[
    list[tuple[str, str, str, str, int, float]],
    list[tuple[str, str, str, str, str, str]],
]:
    contract_list = list(contracts)
    rows: list[tuple[str, str, str, str, int, float]] = []
    failures: list[tuple[str, str, str, str, str, str]] = []

    if not contract_list:
        return rows, failures

    worker_count = max(1, min(max_workers, len(contract_list)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures: list[concurrent.futures.Future[Any]] = []
        for contract in contract_list:
            futures.append(executor.submit(fetch_poly_history_for_contract, contract))
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        for future in concurrent.futures.as_completed(futures):
            contract_rows, contract_failure = future.result()
            rows.extend(contract_rows)
            if contract_failure is not None:
                failures.append(contract_failure)

    rows.sort(key=lambda row: (row[4], row[2], row[3]))
    failures.sort(key=lambda row: (row[1], row[2], row[3]))

    return rows, failures


def fetch_poly_history_for_contract(
    contract: ContractToken,
) -> tuple[
    list[tuple[str, str, str, str, int, float]],
    tuple[str, str, str, str, str, str] | None,
]:
    try:
        payload = http_get_json(
            CLOB_PRICE_HISTORY_URL,
            {
                "market": contract.token_id,
                "startTs": contract.start_ts,
                "endTs": contract.end_ts,
                "fidelity": 1,
            },
        )
    except HTTPError as exc:
        if exc.code != 400:
            raise

        print(
            f"skipping token without price history: market_id={contract.market_id} "
            f"token_id={contract.token_id} side={contract.token_side}",
            file=sys.stderr,
            flush=True,
        )
        return [], (
            contract.event_id,
            contract.market_id,
            contract.token_id,
            contract.token_side,
            type(exc).__name__,
            str(exc),
        )

    history = payload.get("history", []) if isinstance(payload, dict) else []
    minute_prices: dict[int, float] = {}
    for point in history:
        ts = int(point["t"])
        minute_prices[floor_to_minute(ts * 1000)] = float(point["p"])

    contract_rows: list[tuple[str, str, str, str, int, float]] = []
    # 同一分钟可能返回多个点，保留该分钟最后一个价格。
    for ts_ms in sorted(minute_prices):
        contract_rows.append(
            (
                contract.event_id,
                contract.market_id,
                contract.token_id,
                contract.token_side,
                ts_ms,
                minute_prices[ts_ms],
            )
        )

    return contract_rows, None


def fetch_binance_klines(
    start_ts: int,
    end_ts: int,
) -> tuple[list[tuple[int, float, float, float, float, float]], str]:
    try:
        return fetch_binance_futures_klines_via_ccxt(start_ts, end_ts), "binance_usdm_ccxt"
    except ccxt.BaseError as exc:
        print(
            f"falling back to binance spot mirror after ccxt futures failure: {type(exc).__name__}",
            file=sys.stderr,
            flush=True,
        )
        return fetch_binance_spot_klines_via_mirror(start_ts, end_ts), "binance_spot_data_api_fallback"


def fetch_binance_futures_klines_via_ccxt(
    start_ts: int,
    end_ts: int,
) -> list[tuple[int, float, float, float, float, float]]:
    exchange = ccxt.binanceusdm(
        {
            "enableRateLimit": True,
            "timeout": BINANCE_TIMEOUT_MS,
        }
    )
    symbol = "BTC/USDT:USDT"
    since_ms = floor_to_minute(start_ts * 1000)
    end_ms = floor_to_minute(end_ts * 1000)
    rows: list[tuple[int, float, float, float, float, float]] = []

    while since_ms <= end_ms:
        batch = fetch_binance_ohlcv_with_retry(exchange, symbol, since_ms, BINANCE_PAGE_LIMIT)
        if not batch:
            break

        for ts_ms, open_, high, low, close, volume in batch:
            ts_ms = floor_to_minute(int(ts_ms))
            if ts_ms > end_ms:
                continue
            rows.append((ts_ms, float(open_), float(high), float(low), float(close), float(volume)))

        last_ts = int(batch[-1][0])
        next_since = floor_to_minute(last_ts) + 60_000
        if next_since <= since_ms:
            break
        since_ms = next_since
        if last_ts >= end_ms:
            break

    deduped: dict[int, tuple[int, float, float, float, float, float]] = {}
    for row in rows:
        deduped[row[0]] = row
    return [deduped[key] for key in sorted(deduped)]


def fetch_binance_spot_klines_via_mirror(
    start_ts: int,
    end_ts: int,
) -> list[tuple[int, float, float, float, float, float]]:
    since_ms = floor_to_minute(start_ts * 1000)
    end_ms = floor_to_minute(end_ts * 1000)
    rows: list[tuple[int, float, float, float, float, float]] = []

    while since_ms <= end_ms:
        payload = http_get_json(
            BINANCE_SPOT_KLINES_URL,
            {
                "symbol": "BTCUSDT",
                "interval": "1m",
                "startTime": since_ms,
                "endTime": end_ms,
                "limit": BINANCE_SPOT_PAGE_LIMIT,
            },
        )
        if not isinstance(payload, list) or not payload:
            break

        for item in payload:
            ts_ms = floor_to_minute(int(item[0]))
            if ts_ms > end_ms:
                continue
            rows.append(
                (
                    ts_ms,
                    float(item[1]),
                    float(item[2]),
                    float(item[3]),
                    float(item[4]),
                    float(item[5]),
                )
            )

        last_ts = int(payload[-1][0])
        next_since = floor_to_minute(last_ts) + 60_000
        if next_since <= since_ms:
            break
        since_ms = next_since

    deduped: dict[int, tuple[int, float, float, float, float, float]] = {}
    for row in rows:
        deduped[row[0]] = row
    return [deduped[key] for key in sorted(deduped)]


def fetch_binance_ohlcv_with_retry(
    exchange: Any,
    symbol: str,
    since_ms: int,
    limit: int,
) -> list[list[Any]]:
    last_error: Exception | None = None

    for attempt in range(BINANCE_MAX_RETRIES):
        try:
            return exchange.fetch_ohlcv(symbol, timeframe="1m", since=since_ms, limit=limit)
        except (
            ccxt.DDoSProtection,
            ccxt.ExchangeNotAvailable,
            ccxt.NetworkError,
            ccxt.RateLimitExceeded,
            ccxt.RequestTimeout,
        ) as exc:
            if attempt + 1 >= BINANCE_MAX_RETRIES:
                raise
            last_error = exc

        sleep_seconds = retry_sleep_seconds(attempt)
        print(
            f"retrying binance fetch after {type(last_error).__name__}: "
            f"since_ms={since_ms} (sleep={sleep_seconds:.1f}s)",
            file=sys.stderr,
            flush=True,
        )
        time.sleep(sleep_seconds)

    raise RuntimeError(f"unreachable binance retry loop for since_ms={since_ms}")


def build_basis_rows(
    poly_rows: Iterable[tuple[str, str, str, str, int, float]],
    kline_rows: Iterable[tuple[int, float, float, float, float, float]],
) -> list[tuple[str, str, str, str, int, float, float, float]]:
    cex_close_by_minute = {row[0]: row[4] for row in kline_rows}
    out: list[tuple[str, str, str, str, int, float, float, float]] = []

    for event_id, market_id, token_id, token_side, ts_ms, poly_price in poly_rows:
        cex_price = cex_close_by_minute.get(ts_ms)
        if cex_price is None:
            continue
        out.append(
            (
                event_id,
                market_id,
                token_id,
                token_side,
                ts_ms,
                poly_price,
                cex_price,
                poly_price - cex_price,
            )
        )

    return out


def floor_to_minute(ts_ms: int) -> int:
    return (ts_ms // 60_000) * 60_000


def write_duckdb(
    output_path: str,
    events: list[dict[str, Any]],
    contracts: list[ContractToken],
    poly_rows: list[tuple[str, str, str, str, int, float]],
    poly_failures: list[tuple[str, str, str, str, str, str]],
    cex_source: str,
    kline_rows: list[tuple[int, float, float, float, float, float]],
    basis_rows: list[tuple[str, str, str, str, int, float, float, float]],
    market_style: str,
) -> None:
    temp_fd, build_path = tempfile.mkstemp(
        prefix="btc_basis_backtest_",
        suffix=".duckdb",
        dir="/tmp",
    )
    os.close(temp_fd)
    remove_if_exists(build_path)
    remove_if_exists(f"{build_path}.wal")

    conn = duckdb.connect(build_path)
    try:
        duckdb_threads = max(1, min(8, os.cpu_count() or 4))
        conn.execute(f"pragma threads={duckdb_threads}")
        conn.execute(
            """
            create table if not exists gamma_events (
                event_id varchar primary key,
                title varchar,
                slug varchar,
                category varchar,
                start_date timestamptz,
                end_date timestamptz,
                closed_time timestamptz,
                raw_json json
            )
            """
        )
        conn.execute(
            """
            create table if not exists polymarket_contracts (
                event_id varchar,
                market_id varchar,
                condition_id varchar,
                market_question varchar,
                token_id varchar,
                token_side varchar,
                start_ts bigint,
                end_ts bigint
            )
            """
        )
        conn.execute(
            """
            create table if not exists polymarket_price_history (
                event_id varchar,
                market_id varchar,
                token_id varchar,
                token_side varchar,
                ts_ms bigint,
                ts_utc timestamptz,
                poly_price double
            )
            """
        )
        conn.execute(
            """
            create table if not exists polymarket_history_failures (
                event_id varchar,
                market_id varchar,
                token_id varchar,
                token_side varchar,
                error_type varchar,
                error_message varchar
            )
            """
        )
        conn.execute(
            """
            create table if not exists binance_btc_1m (
                ts_ms bigint primary key,
                ts_utc timestamptz,
                open double,
                high double,
                low double,
                close double,
                volume double
            )
            """
        )
        conn.execute(
            """
            create table if not exists build_metadata (
                key varchar primary key,
                value varchar
            )
            """
        )
        conn.execute(
            """
            create table if not exists basis_1m (
                event_id varchar,
                market_id varchar,
                token_id varchar,
                token_side varchar,
                ts_ms bigint,
                ts_utc timestamptz,
                poly_price double,
                cex_price double,
                basis double
            )
            """
        )

        insert_rows_in_batches(
            conn,
            "insert into gamma_events values (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                (
                    str(event.get("id", "")),
                    str(event.get("title", "")),
                    str(event.get("slug", "")),
                    str(event.get("category", "")),
                    to_duck_ts(parse_dt(event.get("startDate"))),
                    to_duck_ts(parse_dt(event.get("endDate"))),
                    to_duck_ts(parse_dt(event.get("closedTime"))),
                    json.dumps(event, ensure_ascii=False),
                )
                for event in events
            ),
            label="gamma_events",
        )
        insert_rows_in_batches(
            conn,
            "insert into polymarket_contracts values (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                (
                    row.event_id,
                    row.market_id,
                    row.condition_id,
                    row.market_question,
                    row.token_id,
                    row.token_side,
                    row.start_ts,
                    row.end_ts,
                )
                for row in contracts
            ),
            label="polymarket_contracts",
        )

        copy_rows_via_temp_csv(
            conn,
            "polymarket_price_history",
            (
                (
                    event_id,
                    market_id,
                    token_id,
                    token_side,
                    ts_ms,
                    to_duck_ts(datetime.fromtimestamp(ts_ms / 1000, tz=UTC)),
                    poly_price,
                )
                for event_id, market_id, token_id, token_side, ts_ms, poly_price in poly_rows
            ),
            label="polymarket_price_history",
        )

        insert_rows_in_batches(
            conn,
            "insert into polymarket_history_failures values (?, ?, ?, ?, ?, ?)",
            poly_failures,
            label="polymarket_history_failures",
        )

        copy_rows_via_temp_csv(
            conn,
            "binance_btc_1m",
            (
                (
                    ts_ms,
                    to_duck_ts(datetime.fromtimestamp(ts_ms / 1000, tz=UTC)),
                    open_,
                    high,
                    low,
                    close,
                    volume,
                )
                for ts_ms, open_, high, low, close, volume in kline_rows
            ),
            label="binance_btc_1m",
        )

        insert_rows_in_batches(
            conn,
            "insert into build_metadata values (?, ?)",
            [
                ("cex_source", cex_source),
                (
                    "discovery_source",
                    "polymarket_public_search_multi_q_price_only"
                    if market_style == "price-only"
                    else "polymarket_public_search_q_btc",
                ),
                ("market_style", market_style),
                ("raw_basis_formula", "poly_price - cex_price"),
                ("signal_basis_formula", "poly_price - cex_probability_proxy"),
            ],
            label="build_metadata",
        )

        copy_rows_via_temp_csv(
            conn,
            "basis_1m",
            (
                (
                    event_id,
                    market_id,
                    token_id,
                    token_side,
                    ts_ms,
                    to_duck_ts(datetime.fromtimestamp(ts_ms / 1000, tz=UTC)),
                    poly_price,
                    cex_price,
                    basis,
                )
                for event_id, market_id, token_id, token_side, ts_ms, poly_price, cex_price, basis in basis_rows
            ),
            label="basis_1m",
        )
    finally:
        conn.close()

    # 临时库先落到 /tmp，确认写完后再复制回正式路径，避免工作区里留下半成品。
    build_wal_path = f"{build_path}.wal"
    output_wal_path = f"{output_path}.wal"
    remove_if_exists(output_wal_path)
    shutil.copy2(build_path, output_path)
    if os.path.exists(build_wal_path):
        shutil.copy2(build_wal_path, output_wal_path)
        remove_if_exists(build_wal_path)
    remove_if_exists(build_path)


def insert_rows_in_batches(
    conn: Any,
    sql: str,
    rows: Iterable[tuple[Any, ...]],
    label: str,
    batch_size: int = DUCKDB_INSERT_BATCH_SIZE,
) -> None:
    batch: list[tuple[Any, ...]] = []
    inserted = 0

    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            conn.executemany(sql, batch)
            inserted += len(batch)
            print(f"wrote {inserted:,} rows into {label}", flush=True)
            batch.clear()

    if batch:
        conn.executemany(sql, batch)
        inserted += len(batch)

    print(f"finished {label}: {inserted:,} rows", flush=True)


def copy_rows_via_temp_csv(
    conn: Any,
    table_name: str,
    rows: Iterable[tuple[Any, ...]],
    label: str,
    progress_every: int = DUCKDB_COPY_PROGRESS_EVERY,
) -> None:
    temp_path = ""
    buffered = 0

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            suffix=".csv",
            delete=False,
        ) as handle:
            temp_path = handle.name
            writer = csv.writer(handle)
            for row in rows:
                writer.writerow(row)
                buffered += 1
                if buffered % progress_every == 0:
                    print(f"buffered {buffered:,} rows for {label}", flush=True)

        escaped_path = temp_path.replace("'", "''")
        print(f"copying {buffered:,} rows into {label}", flush=True)
        conn.execute(f"COPY {table_name} FROM '{escaped_path}' (FORMAT CSV)")
        print(f"finished {label}: {buffered:,} rows", flush=True)
    finally:
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)


def to_duck_ts(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(UTC).isoformat()


def remove_if_exists(path: str) -> None:
    try:
        os.remove(path)
    except FileNotFoundError:
        return


if __name__ == "__main__":
    main()
