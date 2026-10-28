//! Read-only tour of the client against a real Home Assistant instance.
//!
//! Reads `HA_URL`, `HA_TOKEN` and optionally `DEMO_ENTITY` from `config.json`
//! (see `config.example.json`). Nothing in Home Assistant is changed.
//!
//! ```bash
//! RUST_LOG=info,ha_core_client=debug cargo run --example demo
//! ```

use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{TimeDelta, Utc};
use ha_core_client::events::Event;
use ha_core_client::triggers::SunEvent;
use ha_core_client::{ClientConfig, HAClient};
use log::{info, warn};
use serde_json::Value;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let config: Value = serde_json::from_str(&std::fs::read_to_string("config.json").context("reading config.json")?)?;
    let key = |k: &str| config[k].as_str().map(str::to_string);
    let url = key("HA_URL").context("HA_URL missing in config.json")?;
    let token = key("HA_TOKEN").context("HA_TOKEN missing in config.json")?;

    let client = HAClient::connect_with(ClientConfig::new(url, token).request_timeout(Duration::from_secs(15))).await?;
    info!("Connected: {:?}", client.connection_state());

    // --- States ---
    let states = client.get_states().await?;
    info!("{} entities", states.len());
    let entity = key("DEMO_ENTITY")
        .or_else(|| states.iter().find(|s| s.entity_id.starts_with("sensor.")).map(|s| s.entity_id.clone()))
        .context("no entity to demonstrate with")?;

    let state = client.get_state(&entity).await?;
    info!("{entity} = {} ({:?}), friendly name: {:?}", state.state, state.ha_state(), state.attributes.friendly_name());

    let history = client.get_history(&entity, Utc::now() - TimeDelta::hours(1), Utc::now(), false).await?;
    info!("{entity}: {} history entries in the last hour", history.len());
    match client.get_state_at_time(&entity, Utc::now() - TimeDelta::minutes(30), TimeDelta::hours(24)).await {
        Ok(s) => info!("{entity} 30 minutes ago: {}", s.state),
        Err(e) => warn!("{entity} 30 minutes ago: {e}"),
    }

    // --- Subscriptions ---
    client
        .subscribe_entity(&entity, |_client, event| async move {
            match event {
                Event::StateChanged { entity_id, old_state, new_state } => {
                    let old = old_state.map(|s| s.state).unwrap_or_default();
                    let new = new_state.map(|s| s.state).unwrap_or_default();
                    info!("[entity] {entity_id}: {old} -> {new}");
                }
                Event::Status { subscription, status } => {
                    info!("[entity] subscription {} status: {status:?}", subscription.id());
                }
                _ => {}
            }
            Ok(())
        })
        .await?;

    client
        .subscribe_time_pattern_trigger(None, None, Some("/30".into()), |_client, event| async move {
            if let Event::TriggerFired(t) = event {
                info!("[trigger] {}", t.description().unwrap_or("time pattern"));
            }
            Ok(())
        })
        .await?;

    client
        .subscribe_sun_trigger(SunEvent::Sunset, Some(TimeDelta::minutes(-30)), |_client, _event| async move {
            info!("[trigger] 30 minutes before sunset");
            Ok(())
        })
        .await?;

    // Rejected configurations are reported immediately.
    if let Err(e) = client.subscribe_template_trigger("{{ invalid", None, |_, _| async { Ok(()) }).await {
        info!("Invalid template rejected as expected: {e}");
    }

    // --- Connection state ---
    let mut state_rx = client.watch_connection_state();
    tokio::spawn(async move {
        while state_rx.changed().await.is_ok() {
            info!("Connection state: {:?}", *state_rx.borrow());
        }
    });

    info!("Listening; press Ctrl+C to stop.");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        state = client.closed() => warn!("Client stopped: {state:?}"),
    }
    client.stop().await;
    Ok(())
}
