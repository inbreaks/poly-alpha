use anyhow::{anyhow, Context, Result};
use std::env;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::args::BacktestCommand;
use crate::backtest_rust::{
    run_rust_replay_command, run_rust_stress_command, RustReplayCommandArgs, RustStressCommandArgs,
};

const BUILD_DB_SCRIPT: &str = "scripts/build_btc_basis_backtest_db.py";
const REPORT_SCRIPT: &str = "scripts/btc_basis_backtest_report.py";

pub async fn run_backtest_command(command: BacktestCommand) -> Result<()> {
    match command {
        BacktestCommand::PrepareDb {
            start_date,
            end_date,
            output,
            max_events,
            max_contracts,
            allow_spot_fallback,
        } => prepare_db(
            start_date.as_deref(),
            end_date.as_deref(),
            &output,
            max_events,
            max_contracts,
            allow_spot_fallback,
        ),
        BacktestCommand::InspectDb {
            db_path,
            show_failures,
        } => inspect_db(&db_path, show_failures),
        BacktestCommand::Run {
            db_path,
            market_id,
            token_id,
            start,
            end,
            initial_capital,
            rolling_window,
            entry_z,
            exit_z,
            position_units,
            position_notional_usd,
            max_capital_usage,
            cex_hedge_ratio,
            cex_margin_ratio,
            fee_bps,
            slippage_bps,
            report_json,
            equity_csv,
        } => run_report(
            &db_path,
            "run",
            market_id.as_deref(),
            token_id.as_deref(),
            start.as_deref(),
            end.as_deref(),
            initial_capital,
            rolling_window,
            entry_z,
            exit_z,
            position_units,
            position_notional_usd,
            max_capital_usage,
            cex_hedge_ratio,
            cex_margin_ratio,
            fee_bps,
            slippage_bps,
            report_json.as_deref(),
            equity_csv.as_deref(),
        ),
        BacktestCommand::WalkForward {
            db_path,
            market_id,
            token_id,
            start,
            end,
            initial_capital,
            rolling_window,
            entry_z,
            exit_z,
            position_units,
            position_notional_usd,
            max_capital_usage,
            cex_hedge_ratio,
            cex_margin_ratio,
            fee_bps,
            slippage_bps,
            train_days,
            test_days,
            step_days,
            max_slices,
            report_json,
            equity_csv,
        } => run_walk_forward(
            &db_path,
            market_id.as_deref(),
            token_id.as_deref(),
            start.as_deref(),
            end.as_deref(),
            initial_capital,
            rolling_window,
            entry_z,
            exit_z,
            position_units,
            position_notional_usd,
            max_capital_usage,
            cex_hedge_ratio,
            cex_margin_ratio,
            fee_bps,
            slippage_bps,
            train_days,
            test_days,
            step_days,
            max_slices,
            report_json.as_deref(),
            equity_csv.as_deref(),
        ),
        BacktestCommand::RustReplay {
            db_path,
            market_id,
            start,
            end,
            initial_capital,
            rolling_window,
            entry_z,
            exit_z,
            position_notional_usd,
            max_capital_usage,
            cex_hedge_ratio,
            cex_margin_ratio,
            poly_fee_bps,
            poly_slippage_bps,
            cex_fee_bps,
            cex_slippage_bps,
            entry_fill_ratio,
            report_json,
            equity_csv,
            trades_csv,
            snapshots_csv,
        } => {
            run_rust_replay_command(RustReplayCommandArgs {
                db_path,
                market_id,
                start,
                end,
                initial_capital,
                rolling_window,
                entry_z,
                exit_z,
                position_notional_usd,
                max_capital_usage,
                cex_hedge_ratio,
                cex_margin_ratio,
                poly_fee_bps,
                poly_slippage_bps,
                cex_fee_bps,
                cex_slippage_bps,
                entry_fill_ratio,
                report_json,
                equity_csv,
                trades_csv,
                snapshots_csv,
            })
            .await
        }
        BacktestCommand::RustStress {
            db_path,
            market_id,
            start,
            end,
            initial_capital,
            rolling_window,
            entry_z,
            exit_z,
            position_notional_usd,
            max_capital_usage,
            cex_hedge_ratio,
            cex_margin_ratio,
            preset,
            report_json,
        } => {
            run_rust_stress_command(RustStressCommandArgs {
                db_path,
                market_id,
                start,
                end,
                initial_capital,
                rolling_window,
                entry_z,
                exit_z,
                position_notional_usd,
                max_capital_usage,
                cex_hedge_ratio,
                cex_margin_ratio,
                preset,
                report_json,
            })
            .await
        }
    }
}

fn prepare_db(
    start_date: Option<&str>,
    end_date: Option<&str>,
    output: &str,
    max_events: usize,
    max_contracts: usize,
    allow_spot_fallback: bool,
) -> Result<()> {
    let mut args = vec![
        script_path(BUILD_DB_SCRIPT)?,
        "--output".to_owned(),
        output.to_owned(),
    ];
    push_optional_arg(&mut args, "--start-date", start_date);
    push_optional_arg(&mut args, "--end-date", end_date);
    if max_events > 0 {
        args.push("--max-events".to_owned());
        args.push(max_events.to_string());
    }
    if max_contracts > 0 {
        args.push("--max-contracts".to_owned());
        args.push(max_contracts.to_string());
    }
    if allow_spot_fallback {
        args.push("--allow-spot-fallback".to_owned());
    }
    run_python_script(&args)
}

fn inspect_db(db_path: &str, show_failures: bool) -> Result<()> {
    let mut args = vec![
        script_path(REPORT_SCRIPT)?,
        "--db-path".to_owned(),
        db_path.to_owned(),
        "inspect-db".to_owned(),
    ];
    if show_failures {
        args.push("--show-failures".to_owned());
    }
    run_python_script(&args)
}

#[allow(clippy::too_many_arguments)]
fn run_report(
    db_path: &str,
    subcommand: &str,
    market_id: Option<&str>,
    token_id: Option<&str>,
    start: Option<&str>,
    end: Option<&str>,
    initial_capital: Option<f64>,
    rolling_window: Option<usize>,
    entry_z: Option<f64>,
    exit_z: Option<f64>,
    position_units: Option<f64>,
    position_notional_usd: Option<f64>,
    max_capital_usage: Option<f64>,
    cex_hedge_ratio: Option<f64>,
    cex_margin_ratio: Option<f64>,
    fee_bps: Option<f64>,
    slippage_bps: Option<f64>,
    report_json: Option<&str>,
    equity_csv: Option<&str>,
) -> Result<()> {
    let mut args = vec![
        script_path(REPORT_SCRIPT)?,
        "--db-path".to_owned(),
        db_path.to_owned(),
        subcommand.to_owned(),
    ];
    push_optional_arg(&mut args, "--market-id", market_id);
    push_optional_arg(&mut args, "--token-id", token_id);
    push_optional_arg(&mut args, "--start", start);
    push_optional_arg(&mut args, "--end", end);
    push_optional_value(&mut args, "--initial-capital", initial_capital);
    push_optional_value(&mut args, "--rolling-window", rolling_window);
    push_optional_value(&mut args, "--entry-z", entry_z);
    push_optional_value(&mut args, "--exit-z", exit_z);
    push_optional_value(&mut args, "--position-units", position_units);
    push_optional_value(&mut args, "--position-notional-usd", position_notional_usd);
    push_optional_value(&mut args, "--max-capital-usage", max_capital_usage);
    push_optional_value(&mut args, "--cex-hedge-ratio", cex_hedge_ratio);
    push_optional_value(&mut args, "--cex-margin-ratio", cex_margin_ratio);
    push_optional_value(&mut args, "--fee-bps", fee_bps);
    push_optional_value(&mut args, "--slippage-bps", slippage_bps);
    push_optional_arg(&mut args, "--report-json", report_json);
    push_optional_arg(&mut args, "--equity-csv", equity_csv);
    run_python_script(&args)
}

#[allow(clippy::too_many_arguments)]
fn run_walk_forward(
    db_path: &str,
    market_id: Option<&str>,
    token_id: Option<&str>,
    start: Option<&str>,
    end: Option<&str>,
    initial_capital: Option<f64>,
    rolling_window: Option<usize>,
    entry_z: Option<f64>,
    exit_z: Option<f64>,
    position_units: Option<f64>,
    position_notional_usd: Option<f64>,
    max_capital_usage: Option<f64>,
    cex_hedge_ratio: Option<f64>,
    cex_margin_ratio: Option<f64>,
    fee_bps: Option<f64>,
    slippage_bps: Option<f64>,
    train_days: Option<usize>,
    test_days: Option<usize>,
    step_days: Option<usize>,
    max_slices: Option<usize>,
    report_json: Option<&str>,
    equity_csv: Option<&str>,
) -> Result<()> {
    let mut args = vec![
        script_path(REPORT_SCRIPT)?,
        "--db-path".to_owned(),
        db_path.to_owned(),
        "walk-forward".to_owned(),
    ];
    push_optional_arg(&mut args, "--market-id", market_id);
    push_optional_arg(&mut args, "--token-id", token_id);
    push_optional_arg(&mut args, "--start", start);
    push_optional_arg(&mut args, "--end", end);
    push_optional_value(&mut args, "--initial-capital", initial_capital);
    push_optional_value(&mut args, "--rolling-window", rolling_window);
    push_optional_value(&mut args, "--entry-z", entry_z);
    push_optional_value(&mut args, "--exit-z", exit_z);
    push_optional_value(&mut args, "--position-units", position_units);
    push_optional_value(&mut args, "--position-notional-usd", position_notional_usd);
    push_optional_value(&mut args, "--max-capital-usage", max_capital_usage);
    push_optional_value(&mut args, "--cex-hedge-ratio", cex_hedge_ratio);
    push_optional_value(&mut args, "--cex-margin-ratio", cex_margin_ratio);
    push_optional_value(&mut args, "--fee-bps", fee_bps);
    push_optional_value(&mut args, "--slippage-bps", slippage_bps);
    push_optional_value(&mut args, "--train-days", train_days);
    push_optional_value(&mut args, "--test-days", test_days);
    push_optional_value(&mut args, "--step-days", step_days);
    push_optional_value(&mut args, "--max-slices", max_slices);
    push_optional_arg(&mut args, "--report-json", report_json);
    push_optional_arg(&mut args, "--equity-csv", equity_csv);
    run_python_script(&args)
}

fn push_optional_arg(args: &mut Vec<String>, flag: &str, value: Option<&str>) {
    if let Some(value) = value {
        args.push(flag.to_owned());
        args.push(value.to_owned());
    }
}

fn push_optional_value<T: ToString>(args: &mut Vec<String>, flag: &str, value: Option<T>) {
    if let Some(value) = value {
        args.push(flag.to_owned());
        args.push(value.to_string());
    }
}

fn run_python_script(args: &[String]) -> Result<()> {
    let python = resolve_python()?;
    let workspace_root = workspace_root()?;
    let status = Command::new(&python)
        .args(args)
        .current_dir(&workspace_root)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| format!("failed to launch python via `{}`", python.display()))?;

    if status.success() {
        Ok(())
    } else {
        Err(anyhow!("python script exited with status {status}"))
    }
}

fn resolve_python() -> Result<PathBuf> {
    let root = workspace_root()?;
    let mut candidates = Vec::new();
    if let Some(value) = env::var_os("PYTHON") {
        candidates.push(PathBuf::from(value));
    }
    if let Some(venv) = env::var_os("VIRTUAL_ENV") {
        let venv_root = PathBuf::from(venv);
        candidates.push(venv_root.join("bin/python"));
        candidates.push(venv_root.join("bin/python3"));
    }
    candidates.push(root.join(".venv/bin/python"));
    candidates.push(root.join(".venv/bin/python3"));
    candidates.push(PathBuf::from("/usr/bin/python3"));
    candidates.push(PathBuf::from("python3"));
    candidates.push(PathBuf::from("python"));

    for candidate in candidates {
        if python_available(&candidate) {
            return Ok(candidate);
        }
    }
    Err(anyhow!(
        "python runtime not found; expected `.venv/bin/python` or `/usr/bin/python3`"
    ))
}

fn script_path(script: &str) -> Result<String> {
    let path = workspace_root()?.join(Path::new(script));
    if !path.exists() {
        return Err(anyhow!("script not found: {}", path.display()));
    }
    Ok(path.to_string_lossy().into_owned())
}

fn python_available(candidate: &Path) -> bool {
    if candidate.components().count() > 1 && !candidate.exists() {
        return false;
    }

    Command::new(candidate)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn workspace_root() -> Result<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .context("failed to resolve workspace root")?;
    Ok(root)
}
