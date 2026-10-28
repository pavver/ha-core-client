# AI Skill: Home Assistant Core Client (`ha-core-client`)

This document serves as a comprehensive technical guide for AI agents to write applications using the `ha-core-client` library (version 0.2). It contains all public API signatures, data models, and architectural patterns.

## 1. Core Architecture: Handle/Runner Pattern

*   **`HAClient`**: a lightweight, **cloneable** handle. Clone it freely; every clone talks to the same background task.
*   **Background task**: owns the WebSocket, reconnects with backoff, restores subscriptions, pings to detect dead connections.
*   **Lifetime**: the task stops on `stop()`, on a permanently rejected token, or when **every handle is dropped**. Keep a handle alive (e.g. `client.closed().await`).
*   **Callbacks** are `async`, receive their own `HAClient`, and may call the API (no deadlocks). Callbacks of one subscription run **sequentially, in event order**; to process events concurrently, `tokio::spawn` inside the callback.

## 2. Library Setup

```toml
[dependencies]
ha-core-client = { git = "https://github.com/pavver/ha-core-client" }
tokio = { version = "1", features = ["full"] }
anyhow = "1"
chrono = "0.4"
serde_json = "1"
```

Features: `native-tls` (default) or `rustls-tls` for `wss://`.

## 3. Errors

All client methods return `ha_core_client::Result<T>` = `Result<T, ha_core_client::Error>`. `Error` implements `std::error::Error`, so `?` converts it into `anyhow::Error` inside callbacks.

`Error` variants (`#[non_exhaustive]`):
*   `Auth(String)`: token rejected (from `connect`).
*   `Connection(String)`: could not connect (from `connect`).
*   `NotConnected`: the client is reconnecting; nothing was sent. Safe to retry later.
*   `ConnectionLost`: the connection dropped after sending; the outcome is unknown.
*   `Timeout`: no answer within `request_timeout`.
*   `Stopped`: the client was stopped.
*   `Ha { code, message }`: Home Assistant returned an error (e.g. `invalid_format`, `not_found`, `home_assistant_error`).
*   `Protocol(String)`, `Decode(serde_json::Error)`, `InvalidInput(String)`.

## 4. Main API: `HAClient`

### Lifecycle
*   `async fn connect(url: impl Into<String>, token: impl Into<String>) -> Result<HAClient>`: connects and authenticates; the first attempt is not retried.
*   `async fn connect_with(config: ClientConfig) -> Result<HAClient>`.
*   `ClientConfig::new(url, token)` with builder methods `.connect_timeout(Duration)` (10s), `.request_timeout(Duration)` (30s), `.ping_interval(Duration)` (30s), `.pong_timeout(Duration)` (10s), `.reconnect_delay(min, max)` (1s, 60s).
*   `async fn stop(&self)`: stops the client for all handles.
*   `async fn closed(&self) -> ConnectionState`: waits for `Stopped` or `AuthFailed`.
*   `fn connection_state(&self) -> ConnectionState`, `fn watch_connection_state(&self) -> tokio::sync::watch::Receiver<ConnectionState>`.
*   `ConnectionState`: `Connected`, `Reconnecting { attempt: u32 }`, `AuthFailed(String)`, `Stopped`; `is_terminal()`.

### State Operations
*   `async fn get_state(&self, entity_id: &str) -> Result<events::State>`: `Error::Ha { code: "not_found", .. }` if missing.
*   `async fn get_states(&self) -> Result<Vec<events::State>>`.
*   `async fn get_history(&self, entity_id: &str, start: DateTime<Utc>, end: DateTime<Utc>, significant_changes_only: bool) -> Result<Vec<HistoricalState>>`.
*   `async fn get_state_at_time(&self, entity_id: &str, timestamp: DateTime<Utc>, lookback: chrono::TimeDelta) -> Result<events::State>`.

### Service Calls
*   `async fn call_service(&self, domain: impl Into<Domain>, service: impl Into<Service>, entity_id: Option<&str>, data: Option<serde_json::Value>) -> Result<()>`: enums or strings, e.g. `call_service("vacuum", "start", Some("vacuum.robot"), None)`.
*   `async fn call_service_with_response(...same...) -> Result<serde_json::Value>`: returns the service response (e.g. `weather.get_forecasts`).

### Subscriptions
Callback bound: `F: Fn(HAClient, events::Event) -> Fut + Send + Sync + 'static`, `Fut: Future<Output = anyhow::Result<()>> + Send + 'static`. Errors returned by a callback are logged.

Each `subscribe_*` returns `Result<Subscription>` after Home Assistant accepts it (an invalid configuration returns `Error::Ha`).

*   `subscribe_entity(entity_id: &str, cb)`: `Event::StateChanged` for that entity only (state and attribute changes, filtered server-side).
*   `subscribe_events(event_type: &str, cb)`: bus events. `state_changed` → `Event::StateChanged` (**every** entity), `call_service` → `Event::ServiceCalled`, others → `Event::Generic`.
*   `subscribe_service_calls(cb)`: same as `subscribe_events("call_service", cb)`.
*   `subscribe_trigger(trigger: triggers::Trigger, cb)`, `subscribe_trigger_raw(trigger: serde_json::Value, cb)`: `Event::TriggerFired`.
*   `unsubscribe(&self, subscription: Subscription) -> Result<()>`.
*   `Subscription::id() -> u64`, which stays the same across reconnects.

## 5. Automation Triggers

All fire `Event::TriggerFired`.

*   `subscribe_state_trigger(entity_id: &str, to: Option<HaState>, from: Option<HaState>, for_duration: Option<std::time::Duration>, cb)`
*   `subscribe_numeric_state_trigger(entity_id: &str, above: Option<f64>, below: Option<f64>, for_duration: Option<std::time::Duration>, cb)`
*   `subscribe_sun_trigger(event: SunEvent, offset: Option<chrono::TimeDelta>, cb)`: negative offset = before the event, e.g. `TimeDelta::minutes(-30)`.
*   `subscribe_time_trigger(at: impl Into<String>, cb)`, e.g. `"07:30:00"` or an `input_datetime` entity.
*   `subscribe_time_pattern_trigger(hours: Option<String>, minutes: Option<String>, seconds: Option<String>, cb)`, e.g. `minutes: Some("/5".into())`.
*   `subscribe_template_trigger(value_template: impl Into<String>, for_duration: Option<std::time::Duration>, cb)`
*   `subscribe_zone_trigger(entity_id: &str, zone: &str, event: ZoneEvent, cb)`
*   Other platforms (`event`, `mqtt`, `homeassistant`, `calendar`, …, see `TRIGGERS_OVERVIEW.md`): `subscribe_trigger_raw(serde_json::json!({"platform": "mqtt", "topic": "x"}), cb)`.

## 6. Domain Helpers (Recommended)

Use these instead of `call_service` for cleaner code. All return `Result<()>`.

*   **General** (domain taken from the entity id): `turn_on(entity_id)`, `turn_off(entity_id)`, `toggle(entity_id)`.
*   **Climate**: `set_hvac_mode(id, HvacMode)`, `set_temperature(id, f64)`, `set_fan_mode(id, FanMode)`, `set_preset_mode(id, PresetMode)`, `set_swing_mode(id, SwingMode)`.
*   **Cover**: `open_cover(id)`, `close_cover(id)`, `stop_cover(id)`, `set_cover_position(id, u8)`, `set_cover_tilt_position(id, u8)`.
*   **Alarm Control Panel**: `alarm_disarm(id, code: Option<&str>)`, `alarm_arm_home/away/night/vacation(id, code)`.
*   **Lock**: `lock(id, code: Option<&str>)`, `unlock(id, code)`.
*   **Input**: `set_number(id, f64)`, `set_text(id, &str)`, `select_option(id, &str)`. A bare name gets the domain prefix added (`"x"` → `input_number.x`).

## 7. Important Types & Enums

### `ha_core_client::state`
*   `Domain`, `Service`: known variants (`Domain::Light`, `Domain::HomeAssistant`, …, `Service::TurnOn`, `Service::Reload`, …) plus `Other(String)`. `From<&str>`, `Display` gives the wire string, `Domain::of_entity("light.x")`.
*   `HaState`: `On`, `Off`, `Unavailable`, `Unknown`, `Home`, `NotHome`, `Alarm(AlarmState)`, `Climate(HvacMode)`, `Cover(CoverState)`, `Lock(LockState)`, `Number(f64)`, `String(String)`. `HaState::from_string(&str)`; serializes as the plain state string.
*   `climate::{HvacMode, FanMode, SwingMode, PresetMode}`. `HvacMode` is closed; the others have `Other(String)`.
*   `device_class::DeviceClass`, `state_class::StateClass`, `common::UnitOfMeasurement`: all with `Other(String)`.
*   All string enums are `#[non_exhaustive]`: `match` needs a wildcard arm.

### `ha_core_client::events`
*   `Event` (`#[non_exhaustive]`):
    *   `StateChanged { entity_id: String, old_state: Option<State>, new_state: Option<State> }`: `old_state` is `None` if the entity was added, `new_state` is `None` if it was removed.
    *   `ServiceCalled(ServiceCalledEvent { domain, service, service_data })`.
    *   `TriggerFired(TriggerFiredEvent { trigger: Value, context: Option<Value> })` with `platform()`, `description()`, `entity_id()`, `from_state()`, `to_state()`.
    *   `Generic { event_type: String, data: Value }`.
*   `State { entity_id, state: String, attributes: AttributeSet, last_changed, last_updated, last_reported: Option<_>, context: Value }` with `ha_state() -> HaState` and `domain() -> Option<Domain>`.
*   `AttributeSet`: `Climate(ClimateAttributes)` or `Generic(GenericAttributes { friendly_name, unit_of_measurement, device_class, state_class, rest })`; `friendly_name()`. Unknown keys are in `rest`.
*   `HistoricalState { entity_id, state, attributes: HistoricalAttributeSet, last_changed, last_updated }`.

## 8. Implementation Pattern

```rust
use ha_core_client::{HAClient, events::Event, state::HaState};

async fn on_motion(client: HAClient, event: Event) -> anyhow::Result<()> {
    if let Event::StateChanged { new_state: Some(state), .. } = event {
        if state.ha_state() == HaState::On {
            client.turn_on("light.hallway").await?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = HAClient::connect("ws://homeassistant.local:8123/api/websocket", "TOKEN").await?;
    client.subscribe_entity("binary_sensor.motion", on_motion).await?;

    tokio::select! {
        _ = tokio::signal::ctrl_c() => client.stop().await,
        state = client.closed() => anyhow::bail!("client stopped: {state:?}"),
    }
    Ok(())
}
```

## 9. Development Notes
*   **Logging**: through the `log` facade. Use `RUST_LOG=ha_core_client=debug`; `trace` logs raw WebSocket messages (the token is never logged).
*   **Time**: `chrono::DateTime<Utc>` for timestamps, `std::time::Duration` for trigger `for`, `chrono::TimeDelta` for signed offsets and lookbacks.
*   **Retries**: `NotConnected` means nothing was sent, so retrying is safe. `ConnectionLost`/`Timeout` mean the outcome is unknown, so check the state before retrying non-idempotent calls.
