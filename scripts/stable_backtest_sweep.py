from __future__ import annotations

import argparse
import csv
import html
import json
import math
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = ROOT / "data" / "reports" / "stable_sweep"
CLI_BINARY = ROOT / "target" / "debug" / "polyalpha-cli"
DEFAULT_REPORT_OUTPUT = ROOT / "docs" / "stable-sweep-2026-04-05.md"
DEFAULT_CONFIG_OUTPUT = ROOT / "config" / "multi-market-active.stable.toml"
BASE_STABLE_ENV = ROOT / "config" / "multi-market-active.fresh.toml"

ASSET_DATABASES: dict[str, str] = {
    "btc": "data/btc_basis_backtest_price_only_2025_2026.duckdb",
    "eth": "data/eth_basis_backtest_2026.duckdb",
    "sol": "data/sol_basis_backtest_feb_mar.duckdb",
    "xrp": "data/xrp_basis_backtest_feb_mar.duckdb",
}

PHASE1_MIN_PRICES: dict[str, list[float]] = {
    "btc": [0.08, 0.10, 0.14, 0.18, 0.20],
    "eth": [0.06, 0.08, 0.10, 0.14, 0.20],
    "sol": [0.12, 0.14, 0.18, 0.20, 0.24],
    "xrp": [0.06, 0.08, 0.10, 0.14, 0.20],
}
PHASE1_MAX_PRICES = [0.45, 0.50, 0.55, 0.60, 0.65]
PHASE2_ENTRY_Z = [4.0, 4.5, 5.0]
PHASE2_ROLLING_WINDOWS = [360, 600, 900]
PHASE3_EXIT_Z = [0.4, 0.5, 0.6]
PHASE2_TOP_N = 2
PHASE3_TOP_N = 1
MIN_ROUND_TRIP_RATIO = 0.70
DEFAULT_REFERENCE_BANDS: dict[str, dict[str, float | None]] = {
    "btc": {"min_poly_price": 0.20, "max_poly_price": 0.50, "entry_z": None, "rolling_window": None, "exit_z": None},
    "eth": {"min_poly_price": 0.20, "max_poly_price": 0.50, "entry_z": None, "rolling_window": None, "exit_z": None},
    "sol": {"min_poly_price": 0.20, "max_poly_price": 0.50, "entry_z": None, "rolling_window": None, "exit_z": None},
    "xrp": {"min_poly_price": 0.20, "max_poly_price": 0.50, "entry_z": None, "rolling_window": None, "exit_z": None},
}
RECOMMENDED_STABLE_SPECS: dict[str, dict[str, float | None]] = {
    "btc": {"min_poly_price": 0.10, "max_poly_price": 0.45, "entry_z": None, "rolling_window": None, "exit_z": None},
    "eth": {"min_poly_price": 0.14, "max_poly_price": 0.65, "entry_z": 4.0, "rolling_window": 360, "exit_z": 0.6},
    "sol": {"min_poly_price": 0.24, "max_poly_price": 0.65, "entry_z": 4.0, "rolling_window": 600, "exit_z": 0.6},
    "xrp": {"min_poly_price": 0.06, "max_poly_price": 0.65, "entry_z": None, "rolling_window": None, "exit_z": None},
}
BENCHMARK_LABELS = {
    "default": "当前默认",
    "stable": "稳定默认",
    "aggressive": "高收益参考",
}
BENCHMARK_COLORS = {
    "default": "#6c757d",
    "stable": "#0b6e4f",
    "aggressive": "#c84c09",
}


@dataclass(frozen=True)
class SweepRun:
    phase: str
    asset: str
    db_path: str
    min_poly_price: float
    max_poly_price: float
    entry_z: float | None = None
    rolling_window: int | None = None
    exit_z: float | None = None

    def slug(self) -> str:
        parts = [
            self.asset,
            f"min{self.min_poly_price:.2f}",
            f"max{self.max_poly_price:.2f}",
        ]
        if self.entry_z is not None:
            parts.append(f"entry{self.entry_z:.1f}")
        if self.rolling_window is not None:
            parts.append(f"win{self.rolling_window}")
        if self.exit_z is not None:
            parts.append(f"exit{self.exit_z:.1f}")
        return "-".join(part.replace(".", "") for part in parts)


@dataclass(frozen=True)
class SweepRow:
    asset: str
    min_poly_price: float
    max_poly_price: float
    entry_z: float | None
    rolling_window: int | None
    total_pnl: float
    max_drawdown: float
    win_rate: float
    round_trip_count: int
    report_path: str
    phase: str = "phase1"
    exit_z: float | None = None

    @property
    def pnl_over_drawdown(self) -> float:
        if self.max_drawdown <= 0:
            if self.total_pnl > 0:
                return math.inf
            if self.total_pnl < 0:
                return -math.inf
            return 0.0
        return self.total_pnl / self.max_drawdown


def row_from_dict(payload: dict[str, Any]) -> SweepRow:
    return SweepRow(
        asset=str(payload["asset"]),
        min_poly_price=float(payload["min_poly_price"]),
        max_poly_price=float(payload["max_poly_price"]),
        entry_z=None if payload.get("entry_z") is None else float(payload["entry_z"]),
        rolling_window=(
            None if payload.get("rolling_window") is None else int(payload["rolling_window"])
        ),
        total_pnl=float(payload.get("total_pnl", 0.0)),
        max_drawdown=float(payload.get("max_drawdown", 0.0)),
        win_rate=float(payload.get("win_rate", 0.0)),
        round_trip_count=int(payload.get("round_trip_count", 0)),
        report_path=str(payload.get("report_path", "")),
        phase=str(payload.get("phase", "phase1")),
        exit_z=None if payload.get("exit_z") is None else float(payload["exit_z"]),
    )


def format_optional_metric(value: float | None, *, digits: int = 2) -> str:
    if value is None:
        return "默认"
    return f"{value:.{digits}f}"


def row_matches_spec(row: SweepRow, spec: dict[str, float | None]) -> bool:
    def equal_float(left: float | None, right: float | None) -> bool:
        if left is None or right is None:
            return left is right
        return abs(left - right) < 1e-9

    return (
        equal_float(row.min_poly_price, spec["min_poly_price"])
        and equal_float(row.max_poly_price, spec["max_poly_price"])
        and equal_float(row.entry_z, spec["entry_z"])
        and row.rolling_window == spec["rolling_window"]
        and equal_float(row.exit_z, spec["exit_z"])
    )


def pick_row_by_spec(
    rows: list[SweepRow],
    asset: str,
    spec: dict[str, float | None],
) -> SweepRow | None:
    for row in rows:
        if row.asset == asset and row_matches_spec(row, spec):
            return row
    return None


def load_summary_payload(output_dir: Path) -> dict[str, Any]:
    return json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))


def build_selected_recommendations(summary_payload: dict[str, Any]) -> dict[str, dict[str, dict[str, Any]]]:
    rows = [row_from_dict(item) for item in summary_payload["rows"]]
    auto = summary_payload["recommendations"]
    selected: dict[str, dict[str, dict[str, Any]]] = {}
    for asset in ASSET_DATABASES:
        stable_row = pick_row_by_spec(rows, asset, RECOMMENDED_STABLE_SPECS[asset])
        default_row = pick_row_by_spec(rows, asset, DEFAULT_REFERENCE_BANDS[asset])
        aggressive = auto.get(asset, {}).get("aggressive")
        balanced = auto.get(asset, {}).get("balanced")
        selected[asset] = {}
        if default_row is not None:
            selected[asset]["default"] = row_to_summary_dict(default_row)
        if stable_row is not None:
            selected[asset]["stable"] = row_to_summary_dict(stable_row)
        elif balanced is not None:
            selected[asset]["stable"] = balanced
        if aggressive is not None:
            selected[asset]["aggressive"] = aggressive
    return selected


def load_report_payload(report_path: str | Path) -> dict[str, Any]:
    path = Path(report_path)
    if not path.is_absolute():
        path = ROOT / path
    return json.loads(path.read_text(encoding="utf-8"))


def compute_concentration_snapshot(report_path: str | Path) -> dict[str, float]:
    summary = load_report_payload(report_path)["summary"]
    top_entry_markets = summary.get("top_entry_markets", [])
    total_pnl = float(summary.get("total_pnl", 0.0))
    entry_count = int(summary.get("entry_count", 0))
    top5_pnl = sum(max(float(row.get("net_pnl", 0.0)), 0.0) for row in top_entry_markets[:5])
    top3_entries = sum(int(row.get("entry_count", 0)) for row in top_entry_markets[:3])
    return {
        "top5_net_pnl_share": 0.0 if total_pnl <= 0 else top5_pnl / total_pnl,
        "top3_entry_share": 0.0 if entry_count <= 0 else top3_entries / entry_count,
    }


def build_concentration_summary(
    recommendations: dict[str, dict[str, dict[str, Any]]],
) -> dict[str, dict[str, float]]:
    summary: dict[str, dict[str, float]] = {}
    for asset, picks in recommendations.items():
        stable_row = picks.get("stable") or picks.get("balanced") or picks.get("conservative")
        if stable_row is None:
            continue
        snapshot = compute_concentration_snapshot(stable_row["report_path"])
        default_row = picks.get("default")
        if default_row is not None:
            default_snapshot = compute_concentration_snapshot(default_row["report_path"])
            snapshot["default_top5_net_pnl_share"] = default_snapshot["top5_net_pnl_share"]
            snapshot["default_top3_entry_share"] = default_snapshot["top3_entry_share"]
        summary[asset] = snapshot
    return summary


def render_row_brief(row: dict[str, Any]) -> str:
    return (
        f"价格带 {row['min_poly_price']:.2f}-{row['max_poly_price']:.2f}  "
        f"入场Z {format_optional_metric(row.get('entry_z'), digits=1)}  "
        f"出场Z {format_optional_metric(row.get('exit_z'), digits=1)}  "
        f"窗口 {format_optional_metric(row.get('rolling_window'), digits=0)} 分钟  "
        f"PnL {float(row.get('total_pnl', 0.0)):.2f}  "
        f"回撤 {float(row.get('max_drawdown', 0.0)):.2f}  "
        f"胜率 {float(row.get('win_rate', 0.0)):.2%}  "
        f"回合 {int(row.get('round_trip_count', 0))}"
    )


def render_report_document(
    recommendations: dict[str, dict[str, dict[str, Any]]],
    concentration: dict[str, dict[str, float]],
) -> str:
    lines = [
        "# Stable Sweep Report",
        "",
        "## 方法",
        "",
        "- 先扫各币种价格带，再对候选组合继续细化入场 Z、滚动窗口和出场 Z。",
        "- 排序以 `PnL / MaxDrawdown` 为主，兼顾胜率和 round-trip 样本数，避免只靠少数大单。",
        "- 结论优先级不是绝对收益最大，而是更稳定、更分散、能长期复用的默认组合。",
        "",
        "## 建议默认",
        "",
    ]
    for asset in sorted(recommendations):
        picks = recommendations[asset]
        stable = picks.get("stable") or picks.get("balanced") or picks.get("conservative")
        default = picks.get("default")
        aggressive = picks.get("aggressive")
        lines.append(f"### {asset.upper()}")
        lines.append("")
        if default is not None:
            lines.append(f"- 当前默认: {render_row_brief(default)}")
        if stable is not None:
            lines.append(f"- 建议稳定默认: {render_row_brief(stable)}")
        if aggressive is not None:
            lines.append(f"- 高收益参考: {render_row_brief(aggressive)}")
        metrics = concentration.get(asset)
        if metrics is not None:
            lines.append(
                "- 集中度: "
                f"Top5 净收益占比 {metrics.get('top5_net_pnl_share', 0.0):.2%}，"
                f"Top3 开仓占比 {metrics.get('top3_entry_share', 0.0):.2%}"
                + (
                    f"，对比旧默认分别为 {metrics.get('default_top5_net_pnl_share', 0.0):.2%} / "
                    f"{metrics.get('default_top3_entry_share', 0.0):.2%}"
                    if "default_top5_net_pnl_share" in metrics
                    else ""
                )
            )
        lines.append("")
    return "\n".join(lines)


def render_recommended_config(
    recommendations: dict[str, dict[str, dict[str, Any]]],
) -> str:
    lines = [
        "# Stable per-asset basis overrides",
        "# Generated by scripts/stable_backtest_sweep.py",
        "",
    ]
    for asset in sorted(recommendations):
        stable = (
            recommendations[asset].get("stable")
            or recommendations[asset].get("balanced")
            or recommendations[asset].get("conservative")
        )
        if stable is None:
            continue
        lines.append(f"[strategy.basis.overrides.{asset}]")
        lines.append(f'min_poly_price = "{stable["min_poly_price"]:.2f}"')
        lines.append(f'max_poly_price = "{stable["max_poly_price"]:.2f}"')
        if stable.get("entry_z") is not None:
            lines.append(f'entry_z_score_threshold = "{stable["entry_z"]:.1f}"')
        if stable.get("rolling_window") is not None:
            lines.append(f'rolling_window_secs = {int(stable["rolling_window"]) * 60}')
        if stable.get("exit_z") is not None:
            lines.append(f'exit_z_score_threshold = "{stable["exit_z"]:.1f}"')
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_report_and_config(
    report_path: Path,
    config_path: Path,
    recommendations: dict[str, dict[str, dict[str, Any]]],
    concentration: dict[str, dict[str, float]],
) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        render_report_document(recommendations, concentration),
        encoding="utf-8",
    )
    config_path.write_text(
        render_recommended_config(recommendations),
        encoding="utf-8",
    )


def write_full_env_config(
    output_path: Path,
    recommendations: dict[str, dict[str, dict[str, Any]]],
    *,
    base_env_path: Path = BASE_STABLE_ENV,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    base_text = base_env_path.read_text(encoding="utf-8").rstrip()
    overrides_text = render_recommended_config(recommendations).strip()
    output_path.write_text(
        f"{base_text}\n\n{overrides_text}\n",
        encoding="utf-8",
    )


def compute_drawdown_series(equity_values: list[float]) -> list[float]:
    peak = float("-inf")
    drawdown: list[float] = []
    for equity in equity_values:
        peak = max(peak, equity)
        drawdown.append(0.0 if peak == float("-inf") else peak - equity)
    return drawdown


def row_to_run(row: dict[str, Any]) -> SweepRun:
    return SweepRun(
        phase=str(row.get("phase", "phase1")),
        asset=str(row["asset"]),
        db_path=ASSET_DATABASES[str(row["asset"])],
        min_poly_price=float(row["min_poly_price"]),
        max_poly_price=float(row["max_poly_price"]),
        entry_z=None if row.get("entry_z") is None else float(row["entry_z"]),
        rolling_window=(
            None if row.get("rolling_window") is None else int(row["rolling_window"])
        ),
        exit_z=None if row.get("exit_z") is None else float(row["exit_z"]),
    )


def load_equity_curve(equity_csv_path: Path) -> tuple[list[datetime], list[float]]:
    timestamps: list[datetime] = []
    equity: list[float] = []
    with equity_csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            timestamps.append(datetime.fromtimestamp(int(row["ts_ms"]) / 1000, tz=timezone.utc))
            equity.append(float(row["equity"]))
    return timestamps, equity


def utc_now_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def build_phase1_runs() -> list[SweepRun]:
    runs: list[SweepRun] = []
    for asset, min_prices in PHASE1_MIN_PRICES.items():
        for min_price in min_prices:
            for max_price in PHASE1_MAX_PRICES:
                if min_price >= max_price:
                    continue
                runs.append(
                    SweepRun(
                        phase="phase1",
                        asset=asset,
                        db_path=ASSET_DATABASES[asset],
                        min_poly_price=min_price,
                        max_poly_price=max_price,
                    )
                )
    return runs


def select_top_phase1_candidates(
    rows: list[SweepRow],
    top_n: int = PHASE2_TOP_N,
    min_round_trip_ratio: float = MIN_ROUND_TRIP_RATIO,
) -> list[SweepRow]:
    if not rows:
        return []
    max_round_trips = max(row.round_trip_count for row in rows)
    min_round_trips = math.ceil(max_round_trips * min_round_trip_ratio)
    eligible = [row for row in rows if row.round_trip_count >= min_round_trips]
    if not eligible:
        eligible = rows[:]
    eligible.sort(
        key=lambda row: (
            row.pnl_over_drawdown,
            row.win_rate,
            row.total_pnl,
            row.round_trip_count,
        ),
        reverse=True,
    )
    return eligible[:top_n]


def group_rows_by_asset(rows: Iterable[SweepRow]) -> dict[str, list[SweepRow]]:
    grouped: dict[str, list[SweepRow]] = {}
    for row in rows:
        grouped.setdefault(row.asset, []).append(row)
    return grouped


def build_phase2_runs(phase1_rows: list[SweepRow]) -> list[SweepRun]:
    runs: list[SweepRun] = []
    for asset, rows in group_rows_by_asset(phase1_rows).items():
        for row in select_top_phase1_candidates(rows, top_n=PHASE2_TOP_N):
            for entry_z in PHASE2_ENTRY_Z:
                for rolling_window in PHASE2_ROLLING_WINDOWS:
                    runs.append(
                        SweepRun(
                            phase="phase2",
                            asset=asset,
                            db_path=ASSET_DATABASES[asset],
                            min_poly_price=row.min_poly_price,
                            max_poly_price=row.max_poly_price,
                            entry_z=entry_z,
                            rolling_window=rolling_window,
                        )
                    )
    return runs


def select_top_balanced_rows(rows: list[SweepRow], top_n: int) -> list[SweepRow]:
    ordered = rows[:]
    ordered.sort(
        key=lambda row: (
            row.pnl_over_drawdown,
            row.win_rate,
            row.total_pnl,
            row.round_trip_count,
        ),
        reverse=True,
    )
    return ordered[:top_n]


def build_phase3_runs(phase2_rows: list[SweepRow]) -> list[SweepRun]:
    runs: list[SweepRun] = []
    for asset, rows in group_rows_by_asset(phase2_rows).items():
        for row in select_top_balanced_rows(rows, top_n=PHASE3_TOP_N):
            for exit_z in PHASE3_EXIT_Z:
                runs.append(
                    SweepRun(
                        phase="phase3",
                        asset=asset,
                        db_path=ASSET_DATABASES[asset],
                        min_poly_price=row.min_poly_price,
                        max_poly_price=row.max_poly_price,
                        entry_z=row.entry_z,
                        rolling_window=row.rolling_window,
                        exit_z=exit_z,
                    )
                )
    return runs


def build_command(
    run: SweepRun,
    report_path: Path,
    *,
    equity_csv_path: Path | None = None,
) -> list[str]:
    command = [
        str(CLI_BINARY),
        "backtest",
        "rust-replay",
        "--db-path",
        run.db_path,
        "--band-policy",
        "configured_band",
        "--min-poly-price",
        f"{run.min_poly_price:.2f}",
        "--max-poly-price",
        f"{run.max_poly_price:.2f}",
        "--report-json",
        str(report_path),
    ]
    if run.entry_z is not None:
        command.extend(["--entry-z", f"{run.entry_z:.1f}"])
    if run.rolling_window is not None:
        command.extend(["--rolling-window", str(run.rolling_window)])
    if run.exit_z is not None:
        command.extend(["--exit-z", f"{run.exit_z:.1f}"])
    if equity_csv_path is not None:
        command.extend(["--equity-csv", str(equity_csv_path)])
    return command


def parse_report(run: SweepRun, report_path: Path) -> SweepRow:
    payload = json.loads(report_path.read_text())
    summary = payload["summary"]
    return SweepRow(
        asset=run.asset,
        min_poly_price=run.min_poly_price,
        max_poly_price=run.max_poly_price,
        entry_z=run.entry_z,
        rolling_window=run.rolling_window,
        total_pnl=float(summary["total_pnl"]),
        max_drawdown=float(summary["max_drawdown"]),
        win_rate=float(summary["win_rate"]),
        round_trip_count=int(summary["round_trip_count"]),
        report_path=str(report_path.relative_to(ROOT)),
        phase=run.phase,
        exit_z=run.exit_z,
    )


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def execute_run(output_dir: Path, run: SweepRun) -> SweepRow:
    phase_dir = output_dir / run.phase
    log_dir = output_dir / "logs" / run.phase
    ensure_output_dir(phase_dir)
    ensure_output_dir(log_dir)
    report_path = phase_dir / f"{run.slug()}.json"
    if report_path.exists():
        return parse_report(run, report_path)
    log_path = log_dir / f"{run.slug()}.log"
    command = build_command(run, report_path)
    with log_path.open("w", encoding="utf-8") as handle:
        handle.write(" ".join(command))
        handle.write("\n\n")
        handle.flush()
        subprocess.run(
            command,
            cwd=ROOT,
            check=True,
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
    return parse_report(run, report_path)


def run_phase(output_dir: Path, runs: list[SweepRun], max_workers: int) -> list[SweepRow]:
    rows: list[SweepRow] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(execute_run, output_dir, run): run for run in runs}
        for future in as_completed(future_map):
            row = future.result()
            rows.append(row)
            print(
                f"[{row.phase}] {row.asset} min={row.min_poly_price:.2f} max={row.max_poly_price:.2f} "
                f"entry_z={row.entry_z} rolling={row.rolling_window} exit_z={row.exit_z} "
                f"pnl={row.total_pnl:.2f} dd={row.max_drawdown:.2f} wr={row.win_rate:.2%} "
                f"trips={row.round_trip_count}",
                flush=True,
            )
    rows.sort(key=lambda row: (row.asset, row.min_poly_price, row.max_poly_price, row.entry_z or 0.0, row.rolling_window or 0, row.exit_z or 0.0))
    return rows


def row_to_summary_dict(row: SweepRow) -> dict[str, object]:
    return {
        "phase": row.phase,
        "asset": row.asset,
        "min_poly_price": row.min_poly_price,
        "max_poly_price": row.max_poly_price,
        "entry_z": row.entry_z,
        "rolling_window": row.rolling_window,
        "exit_z": row.exit_z,
        "total_pnl": row.total_pnl,
        "max_drawdown": row.max_drawdown,
        "pnl_over_drawdown": row.pnl_over_drawdown,
        "win_rate": row.win_rate,
        "round_trip_count": row.round_trip_count,
        "report_path": row.report_path,
    }


def render_pick_line(label: str, row: SweepRow) -> str:
    return (
        f"- {label}: min={row.min_poly_price:.2f}, max={row.max_poly_price:.2f}, "
        f"entry_z={row.entry_z}, rolling={row.rolling_window}, exit_z={row.exit_z}, "
        f"PnL={row.total_pnl:.2f}, DD={row.max_drawdown:.2f}, PnL/DD={row.pnl_over_drawdown:.2f}, "
        f"WR={row.win_rate:.2%}, RoundTrips={row.round_trip_count}"
    )


def choose_recommendations(rows: list[SweepRow]) -> dict[str, dict[str, SweepRow]]:
    picks: dict[str, dict[str, SweepRow]] = {}
    for asset, asset_rows in group_rows_by_asset(rows).items():
        max_round_trips = max(row.round_trip_count for row in asset_rows)
        min_round_trips = math.ceil(max_round_trips * MIN_ROUND_TRIP_RATIO)
        stable_rows = [row for row in asset_rows if row.round_trip_count >= min_round_trips]
        if not stable_rows:
            stable_rows = asset_rows[:]
        conservative = sorted(
            stable_rows,
            key=lambda row: (row.win_rate, row.pnl_over_drawdown, row.total_pnl),
            reverse=True,
        )[0]
        balanced = sorted(
            stable_rows,
            key=lambda row: (row.pnl_over_drawdown, row.win_rate, row.total_pnl),
            reverse=True,
        )[0]
        aggressive = sorted(
            asset_rows,
            key=lambda row: (row.total_pnl, row.pnl_over_drawdown, row.win_rate),
            reverse=True,
        )[0]
        picks[asset] = {
            "conservative": conservative,
            "balanced": balanced,
            "aggressive": aggressive,
        }
    return picks


def write_summary_files(output_dir: Path, rows: list[SweepRow]) -> None:
    summary_csv = output_dir / "summary.csv"
    summary_json = output_dir / "summary.json"
    summary_md = output_dir / "summary.md"
    ordered_rows = sorted(
        rows,
        key=lambda row: (
            row.asset,
            row.phase,
            row.min_poly_price,
            row.max_poly_price,
            row.entry_z or 0.0,
            row.rolling_window or 0,
            row.exit_z or 0.0,
        ),
    )
    with summary_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row_to_summary_dict(ordered_rows[0]).keys()))
        writer.writeheader()
        for row in ordered_rows:
            writer.writerow(row_to_summary_dict(row))
    summary_payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "ranking_rule": {
            "primary": "pnl_over_drawdown",
            "secondary": "win_rate",
            "eligibility_round_trip_ratio": MIN_ROUND_TRIP_RATIO,
        },
        "rows": [row_to_summary_dict(row) for row in ordered_rows],
        "recommendations": {
            asset: {label: row_to_summary_dict(row) for label, row in picks.items()}
            for asset, picks in choose_recommendations(ordered_rows).items()
        },
    }
    summary_json.write_text(json.dumps(summary_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    lines = [
        "# Stable Sweep Summary",
        "",
        f"- 生成时间(UTC): {summary_payload['generated_at_utc']}",
        f"- 排序主指标: `PnL / MaxDrawdown`",
        f"- 稳定样本门槛: `RoundTrip >= {MIN_ROUND_TRIP_RATIO:.0%}` of asset max",
        "",
    ]
    for asset, picks in choose_recommendations(ordered_rows).items():
        lines.append(f"## {asset.upper()}")
        lines.append("")
        lines.append(render_pick_line("保守", picks["conservative"]))
        lines.append(render_pick_line("平衡", picks["balanced"]))
        lines.append(render_pick_line("激进", picks["aggressive"]))
        lines.append("")
    summary_md.write_text("\n".join(lines), encoding="utf-8")


def write_manifest(output_dir: Path) -> None:
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "phase1_min_prices": PHASE1_MIN_PRICES,
        "phase1_max_prices": PHASE1_MAX_PRICES,
        "phase2_entry_z": PHASE2_ENTRY_Z,
        "phase2_rolling_window": PHASE2_ROLLING_WINDOWS,
        "phase3_exit_z": PHASE3_EXIT_Z,
        "min_round_trip_ratio": MIN_ROUND_TRIP_RATIO,
        "phase2_top_n": PHASE2_TOP_N,
        "phase3_top_n": PHASE3_TOP_N,
        "asset_databases": ASSET_DATABASES,
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def export_artifacts(
    output_dir: Path,
    *,
    report_path: Path = DEFAULT_REPORT_OUTPUT,
    config_path: Path = DEFAULT_CONFIG_OUTPUT,
) -> None:
    summary_payload = load_summary_payload(output_dir)
    recommendations = build_selected_recommendations(summary_payload)
    concentration = build_concentration_summary(recommendations)
    write_report_and_config(report_path, config_path, recommendations, concentration)
    write_full_env_config(config_path, recommendations)


def rerun_with_equity_csv(
    output_dir: Path,
    label: str,
    row: dict[str, Any],
) -> Path:
    benchmark_dir = output_dir / "benchmarks" / row["asset"]
    log_dir = output_dir / "logs" / "benchmarks"
    ensure_output_dir(benchmark_dir)
    ensure_output_dir(log_dir)
    run = row_to_run(row)
    equity_csv_path = benchmark_dir / f"{label}-{run.slug()}.csv"
    if equity_csv_path.exists():
        return equity_csv_path
    report_path = ROOT / str(row["report_path"])
    command = build_command(run, report_path, equity_csv_path=equity_csv_path)
    log_path = log_dir / f"{label}-{run.slug()}.log"
    with log_path.open("w", encoding="utf-8") as handle:
        handle.write(" ".join(command))
        handle.write("\n\n")
        handle.flush()
        subprocess.run(
            command,
            cwd=ROOT,
            check=True,
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
    return equity_csv_path


def plot_timeseries_chart(
    title: str,
    ylabel: str,
    output_png: Path,
    output_svg: Path,
    series_map: dict[str, tuple[list[datetime], list[float]]],
) -> None:
    output_png.parent.mkdir(parents=True, exist_ok=True)
    width, height = 1200, 560
    left, right, top, bottom = 90, 40, 60, 70
    chart_width = width - left - right
    chart_height = height - top - bottom

    all_times = [
        ts.timestamp()
        for timestamps, _ in series_map.values()
        for ts in timestamps
    ]
    all_values = [
        value
        for _, values in series_map.values()
        for value in values
    ]
    if not all_times or not all_values:
        raise RuntimeError("cannot plot empty timeseries")

    min_time = min(all_times)
    max_time = max(all_times)
    if math.isclose(min_time, max_time):
        max_time += 1.0
    min_value = min(all_values)
    max_value = max(all_values)
    if math.isclose(min_value, max_value):
        max_value += 1.0

    def x_pos(ts: datetime) -> float:
        return left + ((ts.timestamp() - min_time) / (max_time - min_time)) * chart_width

    def y_pos(value: float) -> float:
        return top + (1.0 - ((value - min_value) / (max_value - min_value))) * chart_height

    def value_label(index: int, total: int) -> float:
        if total <= 1:
            return min_value
        return min_value + ((max_value - min_value) * index / (total - 1))

    svg_lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#faf8f2" />',
        f'<text x="{left}" y="32" font-size="22" font-family="Helvetica" fill="#1f2933">{html.escape(title)}</text>',
        f'<text x="24" y="{top + chart_height / 2:.1f}" font-size="14" font-family="Helvetica" fill="#52606d" transform="rotate(-90 24 {top + chart_height / 2:.1f})">{html.escape(ylabel)}</text>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + chart_height}" stroke="#7b8794" stroke-width="1"/>',
        f'<line x1="{left}" y1="{top + chart_height}" x2="{left + chart_width}" y2="{top + chart_height}" stroke="#7b8794" stroke-width="1"/>',
    ]

    for idx in range(5):
        y_value = value_label(idx, 5)
        y = y_pos(y_value)
        svg_lines.append(
            f'<line x1="{left}" y1="{y:.2f}" x2="{left + chart_width}" y2="{y:.2f}" stroke="#d9e2ec" stroke-width="1"/>'
        )
        svg_lines.append(
            f'<text x="{left - 10}" y="{y + 5:.2f}" text-anchor="end" font-size="12" font-family="Helvetica" fill="#52606d">{y_value:.2f}</text>'
        )

    tick_count = 5
    for idx in range(tick_count):
        fraction = idx / (tick_count - 1) if tick_count > 1 else 0.0
        tick_time = min_time + (max_time - min_time) * fraction
        x = left + chart_width * fraction
        tick_label = datetime.fromtimestamp(tick_time, tz=timezone.utc).strftime("%Y-%m-%d")
        svg_lines.append(
            f'<line x1="{x:.2f}" y1="{top + chart_height}" x2="{x:.2f}" y2="{top + chart_height + 6}" stroke="#7b8794" stroke-width="1"/>'
        )
        svg_lines.append(
            f'<text x="{x:.2f}" y="{top + chart_height + 24}" text-anchor="middle" font-size="12" font-family="Helvetica" fill="#52606d">{tick_label}</text>'
        )

    legend_y = top
    legend_x = left + chart_width - 220
    for label, (timestamps, values) in series_map.items():
        key = next((name for name, caption in BENCHMARK_LABELS.items() if caption == label), None)
        color = BENCHMARK_COLORS.get(key or "", "#1f77b4")
        points = " ".join(
            f"{x_pos(ts):.2f},{y_pos(value):.2f}"
            for ts, value in zip(timestamps, values)
        )
        svg_lines.append(
            f'<polyline fill="none" stroke="{color}" stroke-width="2.5" points="{points}" />'
        )
        svg_lines.append(
            f'<line x1="{legend_x}" y1="{legend_y}" x2="{legend_x + 18}" y2="{legend_y}" stroke="{color}" stroke-width="3"/>'
        )
        svg_lines.append(
            f'<text x="{legend_x + 26}" y="{legend_y + 4}" font-size="13" font-family="Helvetica" fill="#1f2933">{html.escape(label)}</text>'
        )
        legend_y += 22

    svg_lines.append("</svg>")
    output_svg.write_text("\n".join(svg_lines), encoding="utf-8")

    converter = shutil.which("magick") or shutil.which("convert")
    if converter is None:
        raise RuntimeError("ImageMagick is required to convert SVG charts into PNG")
    subprocess.run([converter, str(output_svg), str(output_png)], check=True, cwd=ROOT)


def export_benchmark_charts(output_dir: Path, max_workers: int = 2) -> None:
    summary_payload = load_summary_payload(output_dir)
    recommendations = build_selected_recommendations(summary_payload)
    chart_dir = output_dir / "charts"
    ensure_output_dir(chart_dir)
    benchmark_tasks: list[tuple[str, str, dict[str, Any]]] = []
    for asset, picks in recommendations.items():
        for label in ("default", "stable", "aggressive"):
            if label in picks:
                benchmark_tasks.append((asset, label, picks[label]))

    benchmark_csvs: dict[tuple[str, str], Path] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(rerun_with_equity_csv, output_dir, label, row): (asset, label)
            for asset, label, row in benchmark_tasks
        }
        for future in as_completed(future_map):
            asset, label = future_map[future]
            benchmark_csvs[(asset, label)] = future.result()

    for asset, picks in recommendations.items():
        benchmark_rows = {
            label: picks[label]
            for label in ("default", "stable", "aggressive")
            if label in picks
        }
        if len(benchmark_rows) < 2:
            continue

        equity_series: dict[str, tuple[list[datetime], list[float]]] = {}
        drawdown_series: dict[str, tuple[list[datetime], list[float]]] = {}
        for label, row in benchmark_rows.items():
            equity_csv_path = benchmark_csvs[(asset, label)]
            timestamps, equity = load_equity_curve(equity_csv_path)
            equity_series[BENCHMARK_LABELS[label]] = (timestamps, equity)
            drawdown_series[BENCHMARK_LABELS[label]] = (
                timestamps,
                compute_drawdown_series(equity),
            )

        plot_timeseries_chart(
            f"{asset.upper()} Equity Curve",
            "Equity (USD)",
            chart_dir / f"{asset}-equity.png",
            chart_dir / f"{asset}-equity.svg",
            equity_series,
        )
        plot_timeseries_chart(
            f"{asset.upper()} Drawdown Curve",
            "Drawdown (USD)",
            chart_dir / f"{asset}-drawdown.png",
            chart_dir / f"{asset}-drawdown.svg",
            drawdown_series,
        )


def print_plan() -> None:
    phase1_runs = build_phase1_runs()
    phase1_counts = {asset: 0 for asset in ASSET_DATABASES}
    for run in phase1_runs:
        phase1_counts[run.asset] += 1
    phase2_count_per_asset = PHASE2_TOP_N * len(PHASE2_ENTRY_Z) * len(PHASE2_ROLLING_WINDOWS)
    phase3_count_per_asset = PHASE3_TOP_N * len(PHASE3_EXIT_Z)
    print("稳定收益优先回测扫面计划")
    print(f"phase1 总数: {len(phase1_runs)}")
    for asset in ASSET_DATABASES:
        print(
            f"  {asset}: min={PHASE1_MIN_PRICES[asset]} max={PHASE1_MAX_PRICES} -> {phase1_counts[asset]} 组"
        )
    print(f"phase2 每资产: top{PHASE2_TOP_N} * {len(PHASE2_ENTRY_Z)} * {len(PHASE2_ROLLING_WINDOWS)} = {phase2_count_per_asset} 组")
    print(f"phase3 每资产: top{PHASE3_TOP_N} * {len(PHASE3_EXIT_Z)} = {phase3_count_per_asset} 组")
    print("排序规则: PnL/DD -> WinRate -> TotalPnL，且 RoundTrip 样本不足的不作为稳定默认候选")


def load_existing_rows(output_dir: Path) -> list[SweepRow]:
    rows: list[SweepRow] = []
    for report_path in sorted(output_dir.glob("phase*/*.json")):
        phase = report_path.parent.name
        payload = json.loads(report_path.read_text())
        summary = payload["summary"]
        name = report_path.stem
        asset = name.split("-")[0]
        min_poly_price = float(name.split("-")[1].replace("min", "")) / 100
        max_poly_price = float(name.split("-")[2].replace("max", "")) / 100
        row = SweepRow(
            asset=asset,
            min_poly_price=min_poly_price,
            max_poly_price=max_poly_price,
            entry_z=None,
            rolling_window=None,
            total_pnl=float(summary["total_pnl"]),
            max_drawdown=float(summary["max_drawdown"]),
            win_rate=float(summary["win_rate"]),
            round_trip_count=int(summary["round_trip_count"]),
            report_path=str(report_path.relative_to(ROOT)),
            phase=phase,
            exit_z=None,
        )
        rows.append(row)
    return rows


def summarize_only(output_dir: Path) -> None:
    rows: list[SweepRow] = []
    for phase_dir in ("phase1", "phase2", "phase3"):
        for report_path in sorted((output_dir / phase_dir).glob("*.json")):
            payload = json.loads(report_path.read_text())
            summary = payload["summary"]
            name = report_path.stem
            asset = name.split("-")[0]
            min_poly_price = float(name.split("-")[1].replace("min", "")) / 100
            max_poly_price = float(name.split("-")[2].replace("max", "")) / 100
            entry_z = None
            rolling_window = None
            exit_z = None
            for part in name.split("-")[3:]:
                if part.startswith("entry"):
                    entry_z = float(part.replace("entry", "")) / 10
                elif part.startswith("win"):
                    rolling_window = int(part.replace("win", ""))
                elif part.startswith("exit"):
                    exit_z = float(part.replace("exit", "")) / 10
            rows.append(
                SweepRow(
                    asset=asset,
                    min_poly_price=min_poly_price,
                    max_poly_price=max_poly_price,
                    entry_z=entry_z,
                    rolling_window=rolling_window,
                    total_pnl=float(summary["total_pnl"]),
                    max_drawdown=float(summary["max_drawdown"]),
                    win_rate=float(summary["win_rate"]),
                    round_trip_count=int(summary["round_trip_count"]),
                    report_path=str(report_path.relative_to(ROOT)),
                    phase=phase_dir,
                    exit_z=exit_z,
                )
            )
    if not rows:
        raise SystemExit(f"no phase reports found under {output_dir}")
    write_summary_files(output_dir, rows)


def default_output_dir() -> Path:
    return DEFAULT_OUTPUT_ROOT / utc_now_slug()


def run_all(output_dir: Path, max_workers: int) -> None:
    ensure_output_dir(output_dir)
    write_manifest(output_dir)
    phase1_rows = run_phase(output_dir, build_phase1_runs(), max_workers=max_workers)
    phase2_rows = run_phase(output_dir, build_phase2_runs(phase1_rows), max_workers=max_workers)
    phase3_rows = run_phase(output_dir, build_phase3_runs(phase2_rows), max_workers=max_workers)
    write_summary_files(output_dir, phase1_rows + phase2_rows + phase3_rows)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the stable-profit backtest sweep")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("plan", help="Print the planned parameter grid")

    run_parser = subparsers.add_parser("run", help="Run all phases and write summaries")
    run_parser.add_argument(
        "--output-dir",
        default=str(default_output_dir()),
        help="Directory for raw reports and summaries",
    )
    run_parser.add_argument(
        "--max-workers",
        type=int,
        default=2,
        help="Number of concurrent backtests to run",
    )

    summarize_parser = subparsers.add_parser("summarize", help="Rebuild summary files from saved reports")
    summarize_parser.add_argument(
        "--output-dir",
        required=True,
        help="Existing sweep output directory",
    )

    export_parser = subparsers.add_parser(
        "export-artifacts",
        help="Generate the saved report document and stable env file from a sweep summary",
    )
    export_parser.add_argument(
        "--output-dir",
        required=True,
        help="Existing sweep output directory",
    )
    export_parser.add_argument(
        "--report-path",
        default=str(DEFAULT_REPORT_OUTPUT),
        help="Markdown report output path",
    )
    export_parser.add_argument(
        "--config-path",
        default=str(DEFAULT_CONFIG_OUTPUT),
        help="Stable env config output path",
    )

    charts_parser = subparsers.add_parser(
        "export-benchmark-charts",
        help="Rerun chosen benchmark combos with equity CSVs and write equity/drawdown charts",
    )
    charts_parser.add_argument(
        "--output-dir",
        required=True,
        help="Existing sweep output directory",
    )
    charts_parser.add_argument(
        "--max-workers",
        type=int,
        default=2,
        help="Number of concurrent benchmark reruns before chart generation",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    if args.command == "plan":
        print_plan()
        return 0
    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = ROOT / output_dir
    if args.command == "run":
        run_all(output_dir, max_workers=args.max_workers)
        print(f"stable sweep completed: {output_dir.relative_to(ROOT)}")
        return 0
    if args.command == "summarize":
        summarize_only(output_dir)
        print(f"stable sweep summary rebuilt: {output_dir.relative_to(ROOT)}")
        return 0
    if args.command == "export-artifacts":
        report_path = Path(args.report_path)
        config_path = Path(args.config_path)
        if not report_path.is_absolute():
            report_path = ROOT / report_path
        if not config_path.is_absolute():
            config_path = ROOT / config_path
        export_artifacts(output_dir, report_path=report_path, config_path=config_path)
        print(
            "stable sweep artifacts written: "
            f"{report_path.relative_to(ROOT)}, {config_path.relative_to(ROOT)}"
        )
        return 0
    if args.command == "export-benchmark-charts":
        export_benchmark_charts(output_dir, max_workers=args.max_workers)
        print(f"stable sweep benchmark charts written: {(output_dir / 'charts').relative_to(ROOT)}")
        return 0
    raise SystemExit(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
