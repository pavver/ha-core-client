# Home Assistant Core Client (ha-core-client)

A strongly-typed, ergonomic, and resilient asynchronous Rust client for the Home Assistant WebSocket API. Built on a Handle/Runner architecture for thread safety and high performance.

## Key Features

*   **Handle/Runner Pattern**: Decouples the user interface (`HAClient`) from the background network task (`ClientRunner`), ensuring thread safety without complex locking.
*   **Strongly Typed**: Utilizes Rust enums for domains, services, states, and triggers, eliminating "magic strings".
*   **Auto-Reconnection**: Automatically handles network failures and restores all active subscriptions upon reconnection.
*   **Rich Trigger Support**: Subscribe to complex Home Assistant triggers including Time, Sun, State, Numeric State, Zone, and Templates.
*   **Historical Data**: Query entity history and retrieve states at specific points in time.
*   **Async First**: Fully powered by `tokio` and `async/await`.

## Architecture

*   **`HAClient`**: The public, cloneable handle for sending commands and subscribing to events.
*   **`events`**: Strongly-typed structures for Home Assistant events and states.
*   **`state`**: Enums for domains, services, and entity states (e.g., `HvacMode`, `LockState`).
*   **`triggers`**: Configurations for automation-like triggers.
*   **`context_helpers`**: Domain-specific ergonomic methods (Climate, Light, Switch, Alarm, etc.).

## Quick Start

Add the library to your `Cargo.toml`:

```toml
[dependencies]
ha-core-client = { git = "https://github.com/your-repo/ha-core-client" }
tokio = { version = "1.0", features = ["full"] }
```

### Basic Example

```rust
use ha_core_client::{HAClient, state::HaState};
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = HAClient::new();
    
    // Connect to Home Assistant
    client.run("ws://192.168.1.100:8123/api/websocket", "YOUR_TOKEN").await?;

    // Subscribe to state changes
    client.subscribe_entity("light.living_room", |client, event| async move {
        println!("State changed: {:?}", event);
        Ok(())
    }).await?;

    // Call a service
    client.turn_on("light.living_room").await?;

    // Get current state
    let state = client.get_state("sensor.temperature").await?;
    println!("Current temperature: {:?}", state.state);

    Ok(())
}
```

## API Overview

### Core `HAClient` Methods

*   `run(url, token)`: Starts the client and establishes the connection.
*   `stop()`: Gracefully shuts down the client.
*   `get_state(entity_id)`: Fetches the current state of a specific entity.
*   `get_states()`: Retrieves states for all entities.
*   `get_history(...)`: Fetches historical state data.
*   `get_state_at_time(entity_id, timestamp, lookback)`: Finds the state at a specific past moment.
*   `call_service(domain, service, entity_id, data)`: Low-level service call.

### Subscriptions

You can subscribe to various event types:
*   `subscribe_entity(entity_id, callback)`: State change events.
*   `subscribe_service_calls(callback)`: All service calls in the system.
*   `subscribe_state_trigger(...)`: Trigger based on state transitions.
*   `subscribe_numeric_state_trigger(...)`: Numeric threshold triggers.
*   `subscribe_sun_trigger(...)`: Sunrise/sunset events with offsets.
*   `subscribe_time_trigger(...)`: Specific time events.

### Domain Helpers

The client provides ergonomic methods for common domains:
*   **General**: `turn_on`, `turn_off`, `toggle`.
*   **Climate**: `set_hvac_mode`, `set_temperature`, `set_fan_mode`, `set_preset_mode`.
*   **Alarm**: `alarm_disarm`, `alarm_arm_home`, `alarm_arm_away`.
*   **Cover**: `open_cover`, `close_cover`, `set_cover_position`.
*   **Lock**: `lock`, `unlock`.
*   **Input**: `set_number`, `set_text`, `select_option`.

## Development

Run the demonstration example to see the client in action:

```bash
cargo run --example demo
```
(Requires a `config.json` in the project root with `HA_URL` and `HA_TOKEN`).

---
Developed as a standalone library for Home Automation in Rust.
