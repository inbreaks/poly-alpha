use anyhow::{anyhow, Context, Result};

use polyalpha_core::Settings;
use polyalpha_pulse::model::PulseAsset;
use polyalpha_pulse::runtime::{PulseBookFeedStub, PulseExecutionMode, PulseRuntimeBuilder};

use crate::args::LiveExecutorMode;

pub async fn run_pulse_paper(env: &str, assets: &[PulseAsset]) -> Result<()> {
    let settings = filtered_pulse_settings(env, assets)?;
    let asset_feeds = assets
        .iter()
        .copied()
        .map(|asset| PulseBookFeedStub { asset, depth: 20 })
        .collect::<Vec<_>>();

    let runtime = PulseRuntimeBuilder::new(settings)
        .with_poly_books(asset_feeds.clone())
        .with_binance_books(asset_feeds)
        .with_execution_mode(PulseExecutionMode::Paper)
        .build()
        .await
        .context("build pulse paper runtime")?;

    println!(
        "pulse paper runtime bootstrap complete: assets={:?}, aggregator={}",
        runtime.enabled_assets(),
        runtime.uses_global_hedge_aggregator()
    );
    Ok(())
}

pub async fn run_pulse_live_mock(
    env: &str,
    assets: &[PulseAsset],
    executor_mode: LiveExecutorMode,
) -> Result<()> {
    if executor_mode != LiveExecutorMode::Mock {
        return Err(anyhow!(
            "pulse live mode currently only supports --executor-mode mock"
        ));
    }

    let settings = filtered_pulse_settings(env, assets)?;
    let asset_feeds = assets
        .iter()
        .copied()
        .map(|asset| PulseBookFeedStub { asset, depth: 20 })
        .collect::<Vec<_>>();

    let runtime = PulseRuntimeBuilder::new(settings)
        .with_poly_books(asset_feeds.clone())
        .with_binance_books(asset_feeds)
        .with_execution_mode(PulseExecutionMode::LiveMock)
        .build()
        .await
        .context("build pulse live-mock runtime")?;

    println!(
        "pulse live-mock runtime bootstrap complete: assets={:?}, aggregator={}",
        runtime.enabled_assets(),
        runtime.uses_global_hedge_aggregator()
    );
    Ok(())
}

pub fn parse_pulse_assets(raw: &str) -> Result<Vec<PulseAsset>> {
    let assets = raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            PulseAsset::from_routing_key(value)
                .ok_or_else(|| anyhow!("unsupported pulse asset `{value}`"))
        })
        .collect::<Result<Vec<_>>>()?;

    if assets.is_empty() {
        return Err(anyhow!("pulse runtime requires at least one asset"));
    }

    Ok(assets)
}

fn filtered_pulse_settings(env: &str, assets: &[PulseAsset]) -> Result<Settings> {
    let mut settings =
        Settings::load(env).with_context(|| format!("failed to load config env `{env}`"))?;

    for (asset_key, route) in settings.strategy.pulse_arb.routing.iter_mut() {
        let enabled_for_selection = PulseAsset::from_routing_key(asset_key)
            .map(|asset| assets.contains(&asset))
            .unwrap_or(false);
        route.enabled = route.enabled && enabled_for_selection;
    }

    Ok(settings)
}
