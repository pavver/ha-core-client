# AI Skill: Home Assistant Core Client (`ha-core-client`)

This document serves as a comprehensive technical guide for AI agents to write applications using the `ha-core-client` library (version 0.3). It contains all public API signatures, data models, and architectural patterns.

## 1. Core Architecture: Handle/Runner Pattern

*   **`HAClient`**: a lightweight, **cloneable** handle. Clone it freely; every clone talks to the same background task.
*   **Background task**: owns the WebSocket, reconnects with backoff, restores subscriptions, pings to detect dead connections.
*   **Lifetime**: the task stops on `stop()`, on a permanently rejected token, or when **every handle is dropped**. Keep a handle alive (e.g. `client.closed().await`).
*   **Callbacks** are `async`, receive their own `HAClient`, and may call the API (no deadlocks). A callback that captures an `HAClient` keeps the client alive, so call `stop()` explicitly in that case. Callbacks of one subscription run **sequentially, in event order**; to process events concurrently, `tokio::spawn` inside the callback. Keep callbacks short: a slow callback makes entity subscriptions merge state changes and other subscriptions drop old events (`SubscriptionStatus::Lagged`), and runs longer than 5s are logged as warnings.

## 2. Library Setup

```toml
[dependencies]
ha-core-client = { git = "https://github.com/pavver/ha-core-client" }
tokio = { version = "1", features = ["full"] }
chrono = "0.4"
serde_json = "1"
```

Requires Home Assistant 2024.10+ (triggers use the `trigger` key; raw triggers should too: `json!({"trigger": "mqtt", ...})`).

Features: `native-tls` (default) or `rustls-tls` for `wss://`.

## 3. Errors

All client methods return `ha_core_client::Result<T>` = `Result<T, ha_core_client::Error>`. `Error` implements `std::error::Error`, so `?` converts it into `BoxError` inside callbacks (and into `anyhow::Error` elsewhere).

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
*   `ClientConfig::new(url, token)` with builder methods `.connect_timeout(Duration)` (10s), `.request_timeout(Duration)` (30s), `.ping_interval(Duration)` (30s), `.pong_timeout(Duration)` (10s, also bounds each write), `.reconnect_delay(min, max)` (1s, 60s), `.subscription_queue_size(usize)` (1024), `.slow_callback_warning(Duration)` (5s; `ZERO` disables), `.max_message_size(usize)` (64 MiB, also bounds frames). Durations are capped at one year (`Duration::MAX` is fine).
*   `async fn stop(&self)`: stops the client for all handles. Subscriptions receive `SubscriptionStatus::Closed`. Waits up to 5s for callbacks to handle queued events (not when called from inside a callback).
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
Callback bound: `F: Fn(HAClient, events::Event) -> Fut + Send + Sync + 'static`, `Fut: Future<Output = ha_core_client::CallbackResult> + Send + 'static`, where `CallbackResult = Result<(), BoxError>` and `BoxError = Box<dyn std::error::Error + Send + Sync>`. A closure ending in `Ok(())` needs no annotation; `?` works on any `std::error::Error` and on `anyhow::Error`. To fail with a message: `return Err("reason".into())`. Errors returned by a callback are logged with their source chain.

Each `subscribe_*` returns `Result<Subscription>` after Home Assistant accepts it (an invalid configuration returns `Error::Ha`). While reconnecting (or if the connection drops before Home Assistant answers), subscribing succeeds: the subscription starts with `Interrupted` and is sent when the connection is back. If the caller stops waiting (cancelled future, timeout), the subscription is cancelled and its callback never runs.

*   `subscribe_entity(entity_id: &str, cb)`: `Event::StateChanged` for that entity only (state and attribute changes, filtered server-side). Same as `subscribe_entity_with(entity_id, EntityChanges::Any, cb)`.
*   `subscribe_entity_with(entity_id: &str, changes: EntityChanges, cb)`: `EntityChanges::State` delivers only changes of the state string (attribute-only updates and queued changes that cancel out, `A → B → A`, are skipped); `EntityChanges::Any` delivers every recorded update. Neither sees a sensor re-reporting the same value with the same attributes: Home Assistant only bumps `last_reported` and does not send it over the WebSocket API (`state_reported` cannot be subscribed to), so "sensor silent for N minutes" logic must not rely on such reports (use `watch_silence` instead). Fails with `Error::Ha { code: "not_found", .. }` if the entity does not exist; use `subscribe_trigger(StateTrigger::new(id), cb)` for entities that may appear later.
*   `subscribe_events(event_type: &str, cb)`: bus events. `state_changed` → `Event::StateChanged` (**every** entity), `call_service` → `Event::ServiceCalled`, others → `Event::Generic`.
*   `subscribe_service_calls(cb)`: same as `subscribe_events("call_service", cb)`.
*   `subscribe_trigger(trigger: impl Into<triggers::Trigger>, cb)`, `subscribe_trigger_raw(trigger: serde_json::Value, cb)`: `Event::TriggerFired`.
*   `subscribe_template(template: &str, cb)`: Home Assistant renders the template and sends `Event::TemplateRendered { result: Value }` right after subscribing, after every reconnect (following `Restored`, possibly unchanged) and whenever the result changes. Re-rendered when a referenced entity changes, and every minute if it uses `now()`. Results are typed (`2`, `true`, `"158 W"`). A syntax error fails the call with `Error::Ha`; a failing render arrives as `Event::TemplateError { message, level }` and the subscription stays.
*   `watch_silence(entity_id: &str, max_silence: std::time::Duration, cb)`: `Event::Silence { entity_id, silent: bool }`: the current value right after subscribing and after every reconnect, then each flip. `silent` is `true` when the entity has not reported for longer than `max_silence`, counting reports of an unchanged value (`last_reported`), and for a missing entity. Built on `subscribe_template`; evaluated in Home Assistant on its clock, on entity changes and every minute, so flips can be up to a minute late. The way to implement "sensor silent for N minutes" without `get_states`. `Error::InvalidInput` for a malformed entity id.
*   `unsubscribe(&self, subscription: Subscription) -> Result<()>`: the callback then gets `SubscriptionStatus::Unsubscribed` as its last event.
*   `Subscription::id() -> u64`, which stays the same across reconnects. Dropping a `Subscription` does not unsubscribe.
*   `Subscription::guard(self, &client) -> SubscriptionGuard`: unsubscribes on drop; `guard.subscription()`, `guard.unsubscribe().await`, `guard.into_inner()`. The guard holds a client handle.

## 5. Automation Triggers

All fire `Event::TriggerFired`. After a reconnect, triggers are re-created in Home Assistant, so a pending `for` countdown restarts.

Builders (preferred), passed to `subscribe_trigger`:

```rust
use ha_core_client::triggers::*;
client.subscribe_trigger(StateTrigger::new("binary_sensor.door").to(HaState::On).for_duration(Duration::from_secs(60)), cb).await?;
client.subscribe_trigger(NumericStateTrigger::new("sensor.temp").above(25.0), cb).await?;
client.subscribe_trigger(SunTrigger::new(SunEvent::Sunset).offset(TimeDelta::minutes(-30)), cb).await?;
client.subscribe_trigger(TimePatternTrigger::new().minutes("/5"), cb).await?;
// also TimeTrigger::new("07:30:00"), TemplateTrigger::new(t).for_duration(d), ZoneTrigger::new(id, "zone.home", ZoneEvent::Enter)
```

*   Other platforms (`event`, `mqtt`, `homeassistant`, `calendar`, …, see `TRIGGERS_OVERVIEW.md`): `subscribe_trigger_raw(serde_json::json!({"trigger": "mqtt", "topic": "x"}), cb)`.

## 6. Domain Helpers (Recommended)

Use these instead of `call_service` for cleaner code. All return `Result<()>`.

*   **General** (domain taken from the entity id): `turn_on(entity_id)`, `turn_off(entity_id)`, `toggle(entity_id)`.
*   **Climate**: `set_hvac_mode(id, HvacMode)`, `set_temperature(id, f64)`, `set_temperature_range(id, low, high)` (`heat_cool`), `set_fan_mode(id, FanMode)`, `set_preset_mode(id, PresetMode)`, `set_swing_mode(id, SwingMode)`.
*   **Cover**: `open_cover(id)`, `close_cover(id)`, `stop_cover(id)`, `set_cover_position(id, u8)`, `set_cover_tilt_position(id, u8)`.
*   **Alarm Control Panel**: `alarm_disarm(id, code: Option<&str>)`, `alarm_arm_home/away/night/vacation(id, code)`.
*   **Lock**: `lock(id, code: Option<&str>)`, `unlock(id, code)`.
*   **Input**: `set_number(id, f64)`, `set_text(id, &str)`, `select_option(id, &str)`. The service domain comes from the entity id (`number.x` → `number.set_value`); a bare name means `input_*` (`"x"` → `input_number.x`).

## 7. Important Types & Enums

### `ha_core_client::state`
*   `Domain`, `Service`: known variants (`Domain::Light`, `Domain::HomeAssistant`, …, `Service::TurnOn`, `Service::Reload`, …) plus `Other(String)`. `From<&str>`, `Display` gives the wire string, `Domain::of_entity("light.x")`. Equality uses the wire string, so `Domain::Other("light".into()) == Domain::Light`.
*   `HaState`: `On`, `Off`, `Unavailable`, `Unknown`, `Home`, `NotHome`, `Alarm(AlarmState)`, `Climate(HvacMode)`, `Cover(CoverState)`, `Lock(LockState)`, `Number(f64)`, `String(String)`. `HaState::from_string(&str)` (domain-agnostic), `HaState::parse_for_domain(&Domain, &str)` (resolves e.g. `open` for lock vs cover); `State::ha_state()` uses the entity's domain. Serializes as the plain state string. `Number(f64)` only when the string prints back unchanged (`"21"`, `"21.5"`); `"21.0"`, `"1e3"`, `"nan"` stay `String`, so for numeric logic parse `state.state` with `.parse::<f64>()`.
*   `climate::{HvacMode, FanMode, SwingMode, PresetMode}`. `HvacMode` is closed; the others have `Other(String)`.
*   `device_class::DeviceClass`, `state_class::StateClass`, `common::UnitOfMeasurement`: all with `Other(String)`.
*   All string enums are `#[non_exhaustive]`: `match` needs a wildcard arm.

### `ha_core_client::events`
*   `Event` (`#[non_exhaustive]`):
    *   `StateChanged { entity_id: String, old_state: Option<State>, new_state: Option<State> }`: `old_state` is `None` if the entity was added, `new_state` is `None` if it was removed. `old_state.state` may equal `new_state.state` (attribute-only change, or merged changes).
    *   `ServiceCalled(ServiceCalledEvent { domain, service, service_data })`.
    *   `TriggerFired(TriggerFiredEvent { trigger: Value, context: Option<Value> })` with `platform()`, `description()`, `entity_id()`, `from_state()`, `to_state()`.
    *   `TemplateRendered { result: Value }`, `TemplateError { message: String, level: String }` (from `subscribe_template`).
    *   `Silence { entity_id: String, silent: bool }` (from `watch_silence`).
    *   `Generic { event_type: String, data: Value }`.
    *   `Status { subscription: Subscription, status: SubscriptionStatus }`: lifecycle of the subscription itself, delivered in order with its data:
        *   `Interrupted`: the connection was lost. No events until `Restored`, so treat derived data as unknown.
        *   `Restored`: active again. For `subscribe_entity`, a change missed while disconnected arrives just before as one `StateChanged` (last known → current; `new_state: None` if the entity was removed).
        *   `Lagged { dropped }`: the callback fell behind. For `subscribe_entity`, `dropped` state changes were merged into the next `StateChanged`; for other subscriptions, the `dropped` oldest events were discarded (queue limit `subscription_queue_size`). Statuses are never dropped.
        *   `Rejected { code, message }`: Home Assistant refused it when restoring after a reconnect. Removed, no more events.
        *   `Closed`: the client stopped. No more events.
        *   `Unsubscribed`: cancelled by `unsubscribe()` or a dropped `SubscriptionGuard`. No more events.
        *   `subscription` can be passed to `client.unsubscribe(...)`, also while offline.
*   `State { entity_id, state: String, attributes: AttributeSet, last_changed, last_updated, last_reported: Option<_>, context: Value }` with `ha_state() -> HaState`, `domain() -> Option<Domain>` and `last_seen() -> DateTime<Utc>` (`last_reported`, or `last_updated` when it is `None`, as in `get_state` and history results).
*   `AttributeSet` (`#[non_exhaustive]`): `Climate(Box<ClimateAttributes>)` (only for `climate.*` entities) or `Generic(GenericAttributes { friendly_name, unit_of_measurement, device_class, state_class, rest })`; `friendly_name()`, `get(key) -> Option<Value>` (any attribute as JSON, `None` if missing or null). Unknown keys are in `rest: serde_json::Map`.
*   `ClimateAttributes { friendly_name, hvac_modes: Option<Vec<HvacMode>> (unknown modes skipped), hvac_action: Option<HvacAction>, current_temperature, current_humidity, temperature, target_temp_low, target_temp_high, target_temp_step, min_temp, max_temp, fan_mode, fan_modes, preset_mode, preset_modes, swing_mode, swing_modes, rest }`. The current HVAC mode is the state (`State::ha_state()`). Structs are `#[non_exhaustive]`: read fields, do not build literals.
*   `HistoricalState { entity_id, state, attributes: AttributeSet, last_changed, last_updated }`; `State::from(historical)`.

## 8. Implementation Pattern

```rust
use ha_core_client::{HAClient, events::Event, state::HaState};

async fn on_motion(client: HAClient, event: Event) -> ha_core_client::CallbackResult {
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
*   **Stale data**: react to `Event::Status` with `SubscriptionStatus::Interrupted`/`Restored` instead of polling `connection_state()`.
*   **Retries**: `NotConnected` means nothing was sent, so retrying is safe. `ConnectionLost`/`Timeout` mean the outcome is unknown, so check the state before retrying non-idempotent calls.
