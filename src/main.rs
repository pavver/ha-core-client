use crate::client::events;
use crate::client::state::{HaState, HvacMode};
use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tracing::{error, info};

mod client;
mod config;

// The `async` callback function for the voltage sensor subscription.
// It now returns a Result, allowing the use of the `?` operator for clean error handling.
async fn voltage_sensor_callback(
    ctx: client::subscription::EventHandlerContext,
    event: events::Event,
) -> Result<()> {
    let merezha_id = "binary_sensor.esphome_power_indicator_merezha";
    let termostat_id = "climate.esphome_gradka1_termostat_obigrivachaww";

    // Ensure we are handling a StateChanged event.
    if let events::Event::StateChanged { new_state, .. } = event {
        info!(
            "State changed for '{}': new state is '{}'",
            new_state.entity_id, new_state.state
        );
    } else {
        // Not a state changed event, so we're done.
        return Ok(());
    }

    // Using `?` to simplify error handling. If `get_state` returns an `Err`,
    // the operator will immediately return the error from this function.
    let state = ctx.get_state(merezha_id).await?;

    info!("Power indicator state is: {}", state);

    // If power is off, turn off the thermostat.
    if state == HaState::On {
        info!("Power is off. Turning off HVAC.");
        ctx.set_hvac_mode(termostat_id, HvacMode::Off).await?;
        info!("HVAC mode set to off.");
    }

    Ok(())
}

// The main function of the application
#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the logger to see output from the client and our application
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Read the configuration from config.json
    let app_config = config::read_config();

    // Create a new instance of the HAClient
    info!("Connecting to Home Assistant at {}", app_config.url);
    let mut client = client::HAClient::new(&app_config.url, &app_config.token)
        .await
        .expect("Failed to create client");

    // Create a closure that wraps our async function.
    // The closure's return type must match the `EventCallback` signature,
    // including the `Result` in the Future's `Output`.
    let subscription_callback =
        Arc::new(|ctx, event| -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
            Box::pin(voltage_sensor_callback(ctx, event))
        });

    // Subscribe to the voltage sensor's state changes with the async callback.
    // Replace "sensor.battery_1_voltage" with the actual entity_id of your voltage sensor.
    client
        .subscribe_entity("sensor.battery_1_voltage", subscription_callback)
        .await
        .expect("Failed to subscribe to entity");

    // Run the client's event loop. This function will run indefinitely,
    // processing messages from Home Assistant and executing callbacks.
    info!("Client is running. Listening for events...");
    if let Err(e) = client.run().await {
        error!("Client event loop failed: {}", e);
    }

    Ok(())
}
