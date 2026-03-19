#!/usr/bin/env python3
"""
按 market 维度对比 Python 回测报告与 Rust replay 报告。

用途：
1. 看两边各自实际交易了多少 market
2. 找出 Python 独有、Rust 独有、以及双方都交易但次数差很大的 market
3. 帮助定位是组合层分配问题，还是单 market 逻辑问题
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PY_REPORT = "data/reports/btc-basis-2025-2026-report-fixed.json"
DEFAULT_RUST_REPORT = "data/reports/rust-replay-report-marketdebug.json"


def resolve_path(raw: str) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def load_python_rows(path: Path) -> dict[str, dict[str, Any]]:
    payload = json.loads(path.read_text())
    return {str(row["market_id"]): row for row in payload["market_rankings"]}


def load_rust_rows(path: Path) -> dict[str, dict[str, Any]]:
    payload = json.loads(path.read_text())
    rows = payload["summary"]["market_debug_rows"]
    return {str(row["market_id"]): row for row in rows}


def print_section(title: str) -> None:
    print()
    print(f"=== {title} ===")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare Python and Rust backtest market stats")
    parser.add_argument("--python-report", default=DEFAULT_PY_REPORT)
    parser.add_argument("--rust-report", default=DEFAULT_RUST_REPORT)
    parser.add_argument("--top", type=int, default=20)
    args = parser.parse_args()

    python_path = resolve_path(args.python_report)
    rust_path = resolve_path(args.rust_report)
    python_rows = load_python_rows(python_path)
    rust_rows = load_rust_rows(rust_path)

    python_traded = {mid for mid, row in python_rows.items() if int(row["round_trip_count"]) > 0}
    rust_traded = {mid for mid, row in rust_rows.items() if int(row["entry_count"]) > 0}

    print("Python 报告:", python_path)
    print("Rust 报告:", rust_path)
    print()
    print(f"Python 实际交易 market 数: {len(python_traded)}")
    print(f"Rust 实际交易 market 数:   {len(rust_traded)}")
    print(f"双方都交易的 market 数:    {len(python_traded & rust_traded)}")
    print(f"仅 Python 交易的 market 数: {len(python_traded - rust_traded)}")
    print(f"仅 Rust 交易的 market 数:   {len(rust_traded - python_traded)}")

    python_only = []
    for market_id in python_traded - rust_traded:
        prow = python_rows[market_id]
        rrow = rust_rows.get(market_id, {})
        python_only.append(
            {
                "market_id": market_id,
                "python_round_trip_count": int(prow["round_trip_count"]),
                "python_trade_count": int(prow["trade_count"]),
                "python_net_pnl": float(prow["net_pnl"]),
                "rust_signal_count": int(rrow.get("signal_count", 0)),
                "rust_rejected_capital_count": int(rrow.get("rejected_capital_count", 0)),
                "rust_net_pnl": float(rrow.get("net_pnl", 0.0)),
                "question": str(prow.get("market_question", "")),
            }
        )
    python_only.sort(
        key=lambda row: (
            row["python_round_trip_count"],
            row["python_trade_count"],
            abs(row["python_net_pnl"]),
        ),
        reverse=True,
    )

    both_gap = []
    for market_id in python_traded & rust_traded:
        prow = python_rows[market_id]
        rrow = rust_rows[market_id]
        both_gap.append(
            {
                "market_id": market_id,
                "round_trip_gap": abs(int(prow["round_trip_count"]) - int(rrow["entry_count"])),
                "python_round_trip_count": int(prow["round_trip_count"]),
                "rust_entry_count": int(rrow["entry_count"]),
                "python_net_pnl": float(prow["net_pnl"]),
                "rust_net_pnl": float(rrow.get("net_pnl", 0.0)),
                "net_pnl_gap": float(prow["net_pnl"]) - float(rrow.get("net_pnl", 0.0)),
                "rust_signal_count": int(rrow["signal_count"]),
                "rust_rejected_capital_count": int(rrow["rejected_capital_count"]),
                "question": str(prow.get("market_question", "")),
            }
        )
    both_gap.sort(
        key=lambda row: (
            row["round_trip_gap"],
            row["python_round_trip_count"],
            abs(row["python_net_pnl"]),
        ),
        reverse=True,
    )

    print_section("Python 独有成交 Top")
    for row in python_only[: args.top]:
        print(
            f"{row['market_id']} | py_rt={row['python_round_trip_count']} | "
            f"py_tr={row['python_trade_count']} | py_pnl={row['python_net_pnl']:.2f} | "
            f"rust_pnl={row['rust_net_pnl']:.2f} | rust_sig={row['rust_signal_count']} | "
            f"rust_rej={row['rust_rejected_capital_count']} | "
            f"{row['question'][:90]}"
        )

    print_section("双方都交易但次数差距最大 Top")
    for row in both_gap[: args.top]:
        print(
            f"{row['market_id']} | gap={row['round_trip_gap']} | "
            f"py_rt={row['python_round_trip_count']} | rust_entry={row['rust_entry_count']} | "
            f"py_pnl={row['python_net_pnl']:.2f} | rust_pnl={row['rust_net_pnl']:.2f} | "
            f"pnl_gap={row['net_pnl_gap']:.2f} | rust_sig={row['rust_signal_count']} | "
            f"rust_rej={row['rust_rejected_capital_count']} | {row['question'][:90]}"
        )


if __name__ == "__main__":
    main()
