# AI Skill: Home Assistant Core Client (`ha-core-client`)

This document serves as a comprehensive technical guide for AI agents to write applications using the `ha-core-client` library. It contains all public API signatures, data models, and architectural patterns.

## 1. Core Architecture: Handle/Runner Pattern

The library uses a **Handle/Runner** pattern.
*   **`HAClient`**: A lightweight, **cloneable** handle. Always clone it and pass it into callbacks.
*   **`ClientRunner`**: A background task that owns the WebSocket connection.
*   **Concurrency**: Callbacks are `async` and receive a `HAClient` handle, allowing them to make API calls (e.g., turn on a light when a sensor trips) without deadlocks.

## 2. Library Setup

### Cargo.toml Dependency
```toml
[dependencies]
ha-core-client = { path = "../ha-core-client" } # Adjust path as needed
tokio = { version = "1.0", features = ["full"] }
anyhow = "1.0"
chrono = { version = "0.4", features = ["serde"] }
serde_json = "1.0"
```

## 3. Main API: `HAClient`

### Lifecycle
*   `fn new() -> Self`: Creates an uninitialized client.
*   `async fn run(&mut self, url: &str, token: &str) -> Result<()>`: Connects and starts the background runner.
*   `async fn stop(&mut self) -> Result<()>`: Gracefully shuts down the client.

### State Operations
*   `async fn get_state(&self, entity_id: &str) -> Result<events::State>`: Fetches current state.
*   `async fn get_states(&self) -> Result<Vec<events::State>>`: Fetches all entity states.
*   `async fn get_history(&self, entity_id: &str, start: DateTime<Utc>, end: DateTime<Utc>, significant_only: Option<bool>) -> Result<Vec<HistoricalState>>`: Fetches history.
*   `async fn get_state_at_time(&self, entity_id: &str, timestamp: DateTime<Utc>, lookback: Duration) -> Result<events::State>`: Finds state at a specific past moment.

### Service Calls
*   `async fn call_service(&self, domain: Domain, service: Service, entity_id: Option<&str>, data: Option<Value>) -> Result<()>`: Generic service call.

### Subscriptions
Signature for callbacks: `Fn(HAClient, events::Event) -> Fut + Send + Sync + 'static` where `Fut: Future<Output = Result<()>> + Send`.

*   `async fn subscribe_entity(&self, entity_id: &str, callback: F) -> Result<Subscription>`: Subscribes to `state_changed`.
*   `async fn subscribe_service_calls(&self, callback: F) -> Result<Subscription>`: Subscribes to all `call_service` events.
*   `async fn unsubscribe(&self, subscription: Subscription) -> Result<()>`: Cancels a subscription.

## 4. Automation Triggers

Triggers allow reacting to Home Assistant's internal automation engine events.

*   `subscribe_state_trigger(entity_id, to, from, for_duration, callback)`
*   `subscribe_numeric_state_trigger(entity_id, above, below, for_duration, callback)`
*   `subscribe_sun_trigger(event: SunEvent, offset, callback)`
*   `subscribe_time_trigger(at: String, callback)` (e.g., "12:00:00")
*   `subscribe_time_pattern_trigger(hours, minutes, seconds, callback)` (Cron-like)
*   `subscribe_template_trigger(value_template: String, for_duration, callback)`
*   `subscribe_zone_trigger(entity_id, zone, event: ZoneEvent, callback)`

## 5. Domain Helpers (Recommended)

Use these instead of `call_service` for cleaner code. They are implemented as methods on `HAClient`.

### General
*   `turn_on(entity_id)`
*   `turn_off(entity_id)`
*   `toggle(entity_id)`

### Climate
*   `set_hvac_mode(entity_id, HvacMode)`
*   `set_temperature(entity_id, f64)`
*   `set_fan_mode(entity_id, FanMode)`
*   `set_preset_mode(entity_id, PresetMode)`
*   `set_swing_mode(entity_id, SwingMode)`

### Cover (Blinds/Shutters)
*   `open_cover(entity_id)`
*   `close_cover(entity_id)`
*   `stop_cover(entity_id)`
*   `set_cover_position(entity_id, u8)`

### Alarm Control Panel
*   `alarm_disarm(entity_id, code: Option<&str>)`
*   `alarm_arm_home/away/night/vacation(entity_id, code)`

### Lock
*   `lock(entity_id, code)`
*   `unlock(entity_id, code)`

### Input Entities
*   `set_number(entity_id, f64)`
*   `set_text(entity_id, &str)`
*   `select_option(entity_id, &str)`

## 6. Important Types & Enums

### `ha_core_client::state`
*   `Domain`: `Light`, `Switch`, `Climate`, `Sensor`, `BinarySensor`, `AlarmControlPanel`, etc.
*   `Service`: `TurnOn`, `TurnOff`, `Toggle`, `SetTemperature`, `Lock`, `Unlock`, etc.
*   `HaState`: `On`, `Off`, `Unavailable`, `Unknown`, `Home`, `NotHome`, `Alarm(AlarmState)`, `Climate(HvacMode)`, `Lock(LockState)`, `Number(f64)`, `String(String)`.

### `ha_core_client::events`
*   `Event`: `StateChanged(StateChangedData)`, `ServiceCalled(ServiceCalledEvent)`, `TriggerFired(TriggerFiredEvent)`, `Generic(Value)`.
*   `State`: Contains `entity_id`, `state: HaState`, `attributes: AttributeSet`, `last_changed: DateTime<Utc>`, `last_updated: DateTime<Utc>`.

## 7. Implementation Pattern

Always follow this structure when writing an app:

```rust
use ha_core_client::{HAClient, events::Event, state::HaState};
use anyhow::Result;

async fn my_handler(client: HAClient, event: Event) -> Result<()> {
    if let Event::StateChanged(data) = event {
        if data.new_state.state == HaState::On {
            client.turn_off("light.other").await?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = HAClient::new();
    client.run("URL", "TOKEN").await?;
    
    client.subscribe_entity("binary_sensor.motion", my_handler).await?;
    
    // Keep the process alive
    tokio::signal::ctrl_c().await?;
    client.stop().await?;
    Ok(())
}
```

## 8. Development Notes
*   **Logging**: Use `RUST_LOG=info` or `debug` to see library activity.
*   **Time**: Use `chrono::DateTime<Utc>` for all timestamps.
*   **Durations**: Triggers use `std::time::Duration`.
