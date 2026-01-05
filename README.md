# ha-core-client

**`ha-core-client`** is a strongly-typed, ergonomic, and resilient asynchronous Rust client for the Home Assistant WebSocket API.

It provides a high-level, concurrent, and easy-to-use interface, allowing you to build powerful and reliable applications that interact with your Home Assistant instance.

## Development Status

This project is currently in active development. While efforts are made to ensure stability and functionality, the API and internal architecture may undergo significant changes in future releases. We appreciate your understanding and welcome feedback during this phase.

[![Crates.io](https://img.shields.io/crates/v/ha-core-client.svg)](https://crates.io/crates/ha-core-client)
[![Docs.rs](https://docs.rs/ha-core-client/badge.svg)](https://docs.rs/ha-core-client)
![License](https://img.shields.io/crates/l/ha-core-client.svg)
<!-- [![Build Status](https://github.com/your-username/ha-core-client/workflows/CI/badge.svg)](https://github.com/your-username/ha-core-client/actions) -->

---

## Why `ha-core-client`?

This library was built from the ground up to provide a developer experience that is:

-   **Type-Safe**: All WebSocket commands, events, and entity states are mapped to strongly-typed Rust structs and enums. This eliminates "magic strings," catches errors at compile time, and enables powerful editor autocompletion.
-   **Ergonomic**: The API is designed to be intuitive and minimize boilerplate. For example, subscription methods accept `async` functions directly as callbacks, handling the complexities of `Arc`, `Box`, and `Pin` internally.
-   **Concurrent**: The client is built on a thread-safe "Handle/Runner" pattern. Callbacks and other tasks can safely make their own API calls back to Home Assistant without causing deadlocks or race conditions.
-   **Resilient**: Features automatic reconnection and re-subscription logic, ensuring your client stays connected even if the Home Assistant instance restarts or the network is unstable.
-   **Flexible**: Provides strongly-typed helper methods for common domains (`climate`, `cover`, `lock`, etc.) while also giving you direct access to the underlying `call_service` function for maximum flexibility.
-   **Comprehensive Logging**: Integrated with the `log` facade and `tracing` backend, allowing for fine-grained control over log verbosity via the `RUST_LOG` environment variable.

## Installation

Add `ha-core-client` to your `Cargo.toml`:

```toml
[dependencies]
ha-core-client = "0.1.0" # Replace with the latest version
tokio = { version = "1", features = ["full"] }
anyhow = "1"
log = "0.4"
tracing = "0.1"
tracing-subscriber = "0.3"
```

## Quick Start

Here is a basic example of how to connect to Home Assistant, subscribe to a sensor's state changes, and call a service.

```rust
use ha_core_client::client::HAClient;
use ha_core_client::client::events::Event;
use anyhow::Result;
use log::{info, error};

// Define an async function to handle state change events
async fn handle_state_changes(
    _client: HAClient,
    event: Event,
) -> Result<()> {
    if let Event::StateChanged(event_data) = event {
        info!(
            "State of '{}' changed to: {}",
            event_data.entity_id,
            event_data.new_state.state
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logger
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // --- Connection Details ---
    let ha_url = "ws://localhost:8123/api/websocket";
    let ha_token = "YOUR_LONG_LIVED_ACCESS_TOKEN";
    
    // --- Create and Run Client ---
    let mut client = HAClient::new();
    client.run(ha_url, ha_token).await?;
    info!("Client is connected and running.");

    // --- Subscribe to an entity ---
    let sensor_id = "sensor.my_temperature";
    info!("Subscribing to state changes for '{}'", sensor_id);
    let subscription = client.subscribe_entity(sensor_id, handle_state_changes).await?;
    
    // --- Call a service ---
    let switch_id = "switch.my_smart_plug";
    info!("Turning on '{}'", switch_id);
    client.turn_on(switch_id).await?;
    
    // --- Keep the client running ---
    // In a real application, you would have a loop or other logic here.
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

    // --- Unsubscribe and shut down ---
    info!("Unsubscribing and shutting down...");
    client.unsubscribe(subscription).await?;
    client.stop().await?;
    info!("Client has been stopped.");

    Ok(())
}
```

## Features

-   **Authentication**: Securely authenticates with Home Assistant using a long-lived access token.
-   **Get All States**: Fetch the current state of every entity (`get_states`).
-   **Get Single State**: Fetch the state of a specific entity (`get_state`).
-   **Get History**: Fetch the historical states for an entity within a given time window (`get_history`).
-   **Get State at Time**: Find the state of an entity at a specific point in the past (`get_state_at_time`).
-   **Call Services**: Call any service with type-safe domains and services (`call_service`).
-   **Domain Helpers**: Ergonomic helper methods for common domains:
    -   `homeassistant`: `turn_on`, `turn_off`, `toggle`
    -   `climate`: `set_hvac_mode`, `set_temperature`, etc.
    -   `cover`: `open_cover`, `close_cover`, `set_cover_position`, etc.
    -   `lock`: `lock`, `unlock`
    -   And more!
-   **Event Subscriptions**: Subscribe to the global event bus for state changes (`subscribe_entity`) or service calls (`subscribe_service_calls`).
-   **Trigger Subscriptions**: Subscribe to a wide variety of Home Assistant's automation triggers, including `state`, `numeric_state`, `sun`, `time`, `template`, and `zone` triggers.

## API Examples

### Subscribe to State Triggers

React to complex events by subscribing to Home Assistant's powerful automation triggers.

```rust
use ha_core_client::client::{HAClient, triggers, state::HaState};
use ha_core_client::client::events::Event;
use anyhow::Result;
use log::info;

async fn handle_trigger(client: HAClient, event: Event) -> Result<()> {
    if let Event::TriggerFired(trigger_event) = event {
        info!("Trigger '{}' fired!", trigger_event.name);
        // You can use the client to perform actions in response
        client.turn_on("light.office").await?;
    }
    Ok(())
}

async fn setup_triggers(client: &HAClient) -> Result<()> {
    // Trigger when a switch is turned on and stays on for 5 seconds
    client.subscribe_state_trigger(
        "switch.my_switch",
        Some(HaState::On),
        None,
        Some(std::time::Duration::from_secs(5)),
        handle_trigger
    ).await?;
    
    // Trigger when a sensor value goes above 25.5
    client.subscribe_numeric_state_trigger(
        "sensor.living_room_temperature",
        Some(25.5),
        None,
        None,
        handle_trigger
    ).await?;

    // Trigger 15 minutes before sunset
    client.subscribe_sun_trigger(
        triggers::SunEvent::Sunset,
        Some(std::time::Duration::from_secs(900)),
        handle_trigger
    ).await?;

    Ok(())
}
```

### Call a Climate Service

Use the strongly-typed helpers to control a climate entity.

```rust
use ha_core_client::client::HAClient;
use ha_core_client::client::state::climate::HvacMode;
use anyhow::Result;

async fn adjust_climate(client: &HAClient) -> Result<()> {
    let climate_id = "climate.living_room";
    
    // Set HVAC mode to "cool"
    client.set_hvac_mode(climate_id, HvacMode::Cool).await?;
    
    // Set target temperature to 21.5 degrees
    client.set_temperature(climate_id, 21.5).await?;

    Ok(())
}
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue for bugs, feature requests, or questions.
