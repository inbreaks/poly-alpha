#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import UTC, datetime
from typing import Any


RUN_ORDER = ["current", "current_noband", "legacy"]
RUN_LABELS = {
    "current": "Current",
    "current_noband": "Current NoBand",
    "legacy": "Legacy",
}
RUN_COLORS = {
    "current": "#0b6bcb",
    "current_noband": "#2f855a",
    "legacy": "#e86a17",
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build current/current-noband/legacy comparison artifacts"
    )
    parser.add_argument("--current-dir", required=True)
    parser.add_argument("--current-noband-dir", required=True)
    parser.add_argument("--legacy-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args(argv)


def load_json(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_equity_curve(path: str) -> list[tuple[int, float]]:
    rows: list[tuple[int, float]] = []
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append((int(float(row["ts_ms"])), float(row["equity"])))
    rows.sort(key=lambda item: item[0])
    return rows


def discover_run_artifacts(run_dir: str) -> dict[str, dict[str, Any]]:
    artifacts: dict[str, dict[str, Any]] = {}
    if not os.path.isdir(run_dir):
        return artifacts
    for name in sorted(os.listdir(run_dir)):
        if not name.endswith("-report.json"):
            continue
        market = name.removesuffix("-report.json")
        report_path = os.path.join(run_dir, name)
        equity_path = os.path.join(run_dir, f"{market}-equity.csv")
        artifacts[market] = {
            "report": load_json(report_path),
            "equity": load_equity_curve(equity_path) if os.path.exists(equity_path) else [],
        }
    return artifacts


def summary_metric(report: dict[str, Any], key: str, default: float = 0.0) -> float:
    return float(report.get("summary", {}).get(key, default))


def summary_value(report: dict[str, Any], key: str, default: Any = None) -> Any:
    return report.get("summary", {}).get(key, default)


def summary_band_policy(report: dict[str, Any]) -> Any:
    summary = report.get("summary", {})
    if "band_policy" in summary:
        return summary.get("band_policy")
    strategy = summary.get("strategy", {})
    if isinstance(strategy, dict) and "band_policy" in strategy:
        return strategy.get("band_policy")
    rust_summary = report.get("rust_replay_report", {}).get("summary", {})
    if isinstance(rust_summary, dict) and "band_policy" in rust_summary:
        return rust_summary.get("band_policy")
    return None


def build_run_row(run_kind: str, report: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_kind": run_kind,
        "label": RUN_LABELS[run_kind],
        "total_pnl": summary_metric(report, "total_pnl"),
        "ending_equity": summary_metric(report, "ending_equity"),
        "win_rate": summary_metric(report, "win_rate"),
        "max_drawdown": summary_metric(report, "max_drawdown"),
        "trade_count": int(summary_metric(report, "trade_count")),
        "total_fees_paid": summary_metric(report, "total_fees_paid"),
        "total_slippage_paid": summary_metric(report, "total_slippage_paid"),
        "band_policy": summary_band_policy(report),
    }


def build_summary_payload(
    *,
    current_report: dict[str, Any],
    current_noband_report: dict[str, Any],
    legacy_report: dict[str, Any],
) -> dict[str, Any]:
    return {
        "rows": [
            build_run_row("current", current_report),
            build_run_row("current_noband", current_noband_report),
            build_run_row("legacy", legacy_report),
        ]
    }


def build_market_rows(
    current: dict[str, dict[str, Any]],
    current_noband: dict[str, dict[str, Any]],
    legacy: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    markets = sorted(set(current) | set(current_noband) | set(legacy))
    rows: list[dict[str, Any]] = []
    for market in markets:
        current_report = current.get(market, {}).get("report", {})
        current_noband_report = current_noband.get(market, {}).get("report", {})
        legacy_report = legacy.get(market, {}).get("report", {})
        rows.append(
            {
                "market": market,
                "current_total_pnl": summary_metric(current_report, "total_pnl"),
                "current_noband_total_pnl": summary_metric(
                    current_noband_report, "total_pnl"
                ),
                "legacy_total_pnl": summary_metric(legacy_report, "total_pnl"),
                "current_minus_legacy_pnl_delta": summary_metric(current_report, "total_pnl")
                - summary_metric(legacy_report, "total_pnl"),
                "current_noband_minus_legacy_pnl_delta": summary_metric(
                    current_noband_report, "total_pnl"
                )
                - summary_metric(legacy_report, "total_pnl"),
                "current_ending_equity": summary_metric(current_report, "ending_equity"),
                "current_noband_ending_equity": summary_metric(
                    current_noband_report, "ending_equity"
                ),
                "legacy_ending_equity": summary_metric(legacy_report, "ending_equity"),
                "current_win_rate_pct": summary_metric(current_report, "win_rate") * 100.0,
                "current_noband_win_rate_pct": summary_metric(
                    current_noband_report, "win_rate"
                )
                * 100.0,
                "legacy_win_rate_pct": summary_metric(legacy_report, "win_rate") * 100.0,
                "current_max_drawdown": summary_metric(current_report, "max_drawdown"),
                "current_noband_max_drawdown": summary_metric(
                    current_noband_report, "max_drawdown"
                ),
                "legacy_max_drawdown": summary_metric(legacy_report, "max_drawdown"),
                "current_trade_count": int(summary_metric(current_report, "trade_count")),
                "current_noband_trade_count": int(
                    summary_metric(current_noband_report, "trade_count")
                ),
                "legacy_trade_count": int(summary_metric(legacy_report, "trade_count")),
                "current_total_fees_paid": summary_metric(
                    current_report, "total_fees_paid"
                ),
                "current_noband_total_fees_paid": summary_metric(
                    current_noband_report, "total_fees_paid"
                ),
                "legacy_total_fees_paid": summary_metric(legacy_report, "total_fees_paid"),
                "current_total_slippage_paid": summary_metric(
                    current_report, "total_slippage_paid"
                ),
                "current_noband_total_slippage_paid": summary_metric(
                    current_noband_report, "total_slippage_paid"
                ),
                "legacy_total_slippage_paid": summary_metric(
                    legacy_report, "total_slippage_paid"
                ),
                "current_band_policy": summary_band_policy(current_report),
                "current_noband_band_policy": summary_band_policy(current_noband_report),
                "legacy_band_policy": summary_band_policy(legacy_report),
                "current_start_ts_ms": summary_value(current_report, "start_ts_ms"),
                "current_noband_start_ts_ms": summary_value(
                    current_noband_report, "start_ts_ms"
                ),
                "legacy_start_ts_ms": summary_value(legacy_report, "start_ts_ms"),
                "current_end_ts_ms": summary_value(current_report, "end_ts_ms"),
                "current_noband_end_ts_ms": summary_value(
                    current_noband_report, "end_ts_ms"
                ),
                "legacy_end_ts_ms": summary_value(legacy_report, "end_ts_ms"),
            }
        )
    return rows


def build_aggregate_payload(market_rows: list[dict[str, Any]]) -> dict[str, Any]:
    def aggregate_report(prefix: str) -> dict[str, Any]:
        return {
            "summary": {
                "total_pnl": sum(row[f"{prefix}_total_pnl"] for row in market_rows),
                "ending_equity": sum(row[f"{prefix}_ending_equity"] for row in market_rows),
                "trade_count": sum(row[f"{prefix}_trade_count"] for row in market_rows),
                "total_fees_paid": sum(
                    row[f"{prefix}_total_fees_paid"] for row in market_rows
                ),
                "total_slippage_paid": sum(
                    row[f"{prefix}_total_slippage_paid"] for row in market_rows
                ),
                "band_policy": (
                    market_rows[0].get(f"{prefix}_band_policy") if market_rows else None
                ),
                "win_rate": 0.0,
                "max_drawdown": max(
                    (row[f"{prefix}_max_drawdown"] for row in market_rows),
                    default=0.0,
                ),
            }
        }

    return build_summary_payload(
        current_report=aggregate_report("current"),
        current_noband_report=aggregate_report("current_noband"),
        legacy_report=aggregate_report("legacy"),
    )


def aggregate_run_equity_series(
    artifacts: dict[str, dict[str, Any]],
    *,
    run_kind: str,
    all_ts: list[int],
) -> list[dict[str, Any]]:
    cursors = {market: 0 for market in artifacts}
    carried = {
        market: (artifact.get("equity", [(0, 0.0)])[0][1] if artifact.get("equity") else 0.0)
        for market, artifact in artifacts.items()
    }
    initial_equity = sum(
        artifact.get("equity", [(0, 0.0)])[0][1]
        for artifact in artifacts.values()
        if artifact.get("equity")
    )
    rows: list[dict[str, Any]] = []
    for ts_ms in all_ts:
        total_equity = 0.0
        for market, artifact in artifacts.items():
            points = artifact.get("equity", [])
            cursor = cursors[market]
            while cursor < len(points) and points[cursor][0] <= ts_ms:
                carried[market] = points[cursor][1]
                cursor += 1
            cursors[market] = cursor
            total_equity += carried[market]
        rows.append(
            {
                "kind": run_kind,
                "ts_ms": ts_ms,
                "equity": total_equity,
                "pnl": total_equity - initial_equity,
            }
        )
    return rows


def build_aggregate_equity_rows(
    current: dict[str, dict[str, Any]],
    current_noband: dict[str, dict[str, Any]],
    legacy: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    run_map = {
        "current": current,
        "current_noband": current_noband,
        "legacy": legacy,
    }
    all_ts = sorted(
        {
            ts_ms
            for artifacts in run_map.values()
            for artifact in artifacts.values()
            for ts_ms, _equity in artifact.get("equity", [])
        }
    )
    rows: list[dict[str, Any]] = []
    for run_kind in RUN_ORDER:
        rows.extend(
            aggregate_run_equity_series(
                run_map[run_kind], run_kind=run_kind, all_ts=all_ts
            )
        )
    return rows


def write_csv(path: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: str, payload: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def load_plot_modules() -> tuple[Any, Any, Any]:
    import matplotlib

    matplotlib.use("Agg")
    matplotlib.rcParams["svg.fonttype"] = "none"

    from matplotlib import dates as mdates
    from matplotlib import pyplot as plt
    from matplotlib.ticker import MaxNLocator

    return plt, mdates, MaxNLocator


def save_chart(fig: Any, output_stem: str) -> None:
    fig.savefig(f"{output_stem}.svg", format="svg", bbox_inches="tight")
    fig.savefig(f"{output_stem}.png", format="png", dpi=160, bbox_inches="tight")
    plt, _mdates, _locator = load_plot_modules()
    plt.close(fig)


def save_aggregate_chart(rows: list[dict[str, Any]], output_stem: str) -> None:
    plt, mdates, MaxNLocator = load_plot_modules()

    grouped: dict[str, list[dict[str, Any]]] = {run_kind: [] for run_kind in RUN_ORDER}
    for row in rows:
        grouped[row["kind"]].append(row)

    fig, ax = plt.subplots(figsize=(14, 5.2))
    fig.patch.set_facecolor("#f4f7fb")
    ax.set_facecolor("#ffffff")

    for run_kind in RUN_ORDER:
        series = sorted(grouped[run_kind], key=lambda item: item["ts_ms"])
        if not series:
            continue
        x_values = [
            datetime.fromtimestamp(item["ts_ms"] / 1000.0, tz=UTC) for item in series
        ]
        y_values = [float(item["pnl"]) for item in series]
        ax.plot(
            x_values,
            y_values,
            color=RUN_COLORS[run_kind],
            linewidth=2.2,
            label=RUN_LABELS[run_kind],
        )

    ax.set_title(
        "Current vs Current NoBand vs Legacy\nAggregate equity / pnl comparison across all discovered markets.",
        loc="left",
        fontsize=16,
        fontweight="bold",
        color="#102a43",
    )
    ax.set_ylabel("PnL (USD)")
    ax.set_xlabel("Timeline")
    ax.grid(axis="y", color="#e9eff5")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#bcccdc")
    ax.spines["bottom"].set_color("#bcccdc")
    ax.yaxis.set_major_locator(MaxNLocator(6))
    locator = mdates.AutoDateLocator(minticks=4, maxticks=8)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
    ax.legend(loc="upper left", frameon=False)
    fig.tight_layout()
    save_chart(fig, output_stem)

def save_per_market_chart(rows: list[dict[str, Any]], output_stem: str) -> None:
    plt, _mdates, MaxNLocator = load_plot_modules()

    fig, ax = plt.subplots(figsize=(14, 5.2))
    fig.patch.set_facecolor("#f4f7fb")
    ax.set_facecolor("#ffffff")

    if rows:
        group_centers = list(range(len(rows)))
        group_width = 0.75
        bar_width = group_width / len(RUN_ORDER)
        for offset, run_kind in enumerate(RUN_ORDER):
            x_values = [
                center - group_width / 2.0 + offset * bar_width + bar_width / 2.0
                for center in group_centers
            ]
            y_values = [float(row[f"{run_kind}_total_pnl"]) for row in rows]
            ax.bar(
                x_values,
                y_values,
                width=bar_width * 0.9,
                color=RUN_COLORS[run_kind],
                label=RUN_LABELS[run_kind],
            )
        ax.set_xticks(group_centers)
        ax.set_xticklabels([row["market"] for row in rows], rotation=20, ha="right")

    ax.set_title(
        "Per-Market PnL Comparison\nGrouped market-level total pnl for Current / Current NoBand / Legacy.",
        loc="left",
        fontsize=16,
        fontweight="bold",
        color="#102a43",
    )
    ax.set_ylabel("Total PnL (USD)")
    ax.axhline(0.0, color="#7b8794", linewidth=1.0)
    ax.grid(axis="y", color="#e9eff5")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#bcccdc")
    ax.spines["bottom"].set_color("#bcccdc")
    ax.yaxis.set_major_locator(MaxNLocator(6))
    ax.legend(loc="upper left", frameon=False)
    fig.tight_layout()
    save_chart(fig, output_stem)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    current = discover_run_artifacts(args.current_dir)
    current_noband = discover_run_artifacts(args.current_noband_dir)
    legacy = discover_run_artifacts(args.legacy_dir)

    market_rows = build_market_rows(current, current_noband, legacy)
    aggregate_payload = build_aggregate_payload(market_rows)
    aggregate_equity_rows = build_aggregate_equity_rows(current, current_noband, legacy)

    output_dir = args.output_dir
    charts_dir = os.path.join(output_dir, "charts")
    os.makedirs(charts_dir, exist_ok=True)

    write_csv(os.path.join(output_dir, "summary.csv"), market_rows)
    write_json(
        os.path.join(output_dir, "summary.json"),
        {"rows": aggregate_payload["rows"], "markets": market_rows},
    )
    write_csv(os.path.join(output_dir, "aggregate_equity.csv"), aggregate_equity_rows)

    save_aggregate_chart(
        aggregate_equity_rows,
        os.path.join(charts_dir, "aggregate-current-vs-current-noband-vs-legacy"),
    )
    save_per_market_chart(
        market_rows,
        os.path.join(charts_dir, "per-market-current-vs-current-noband-vs-legacy"),
    )


if __name__ == "__main__":
    main()
