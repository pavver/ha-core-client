# AI Skill: Home Assistant Core Client (`ha-core-client`)

This document serves as a comprehensive technical guide for AI agents to write applications using the `ha-core-client` library (version 0.4). It contains all public API signatures, data models, and architectural patterns.

## 1. Core Architecture: Handle/Runner Pattern

*   **`HAClient`**: a lightweight, **cloneable** handle. Clone it freely; every clone talks to the same background task.
*   **Background task**: owns the WebSocket, reconnects with backoff, restores subscriptions, pings to detect dead connections.
*   **Lifetime**: the task stops on `stop()`, on a permanently rejected token, or when **every handle is dropped**. Keep a handle alive (e.g. `client.closed().await`).
*   **Callbacks** are `async`, receive their own `HAClient`, and may call the API (no deadlocks). A callback that captures an `HAClient` keeps the client alive, so call `stop()` explicitly in that case. Callbacks of one subscription run **sequentially, in event order**; to process events concurrently, `tokio::spawn` inside the callback. Keep callbacks short: a slow callback makes entity subscriptions merge state changes (unless `Source::without_merging()`) and other subscriptions drop old events (`SubscriptionStatus::Lagged`), and runs longer than 5s are logged as warnings.

## 2. Library Setup

```toml
[dependencies]
ha-core-client = { git = "https://github.com/pavver/ha-core-client", tag = "v0.4.0" }
tokio = { version = "1", features = ["full"] }
chrono = "0.4"
serde_json = "1"
```

Requires Home Assistant 2024.10+ (triggers use the `trigger` key; raw triggers should too: `json!({"trigger": "mqtt", ...})`).

Features: `native-tls` (default) or `rustls-tls` for `wss://`. For a private CA or self-signed certificate pass a custom connector with `ClientConfig::tls_connector(TlsConnector::NativeTls(..))` (or `Rustls(..)`); `TlsConnector` is a re-export of `tokio_tungstenite::Connector`, and you add `native-tls`/`rustls` as your own dependency to build it.

## 3. Errors

All client methods return `ha_core_client::Result<T>` = `Result<T, ha_core_client::Error>`. `Error` implements `std::error::Error`, so `?` converts it into `BoxError` inside callbacks (and into `anyhow::Error` elsewhere).

`Error` variants (`#[non_exhaustive]`):
*   `Auth(String)`: token rejected (from `connect`).
*   `Connection(String)`: could not connect (from `connect`).
*   `NotConnected`: the client is reconnecting; nothing was sent. Safe to retry later. With `wait_for_connection` set, request-style calls wait up to that long for the connection first, and only then fail with this error.
*   `ConnectionLost`: the connection dropped after sending; the outcome is unknown.
*   `Timeout`: no answer within `request_timeout`.
*   `Stopped`: the client was stopped.
*   `Ha { code, message, translation }`: Home Assistant returned an error (`code: HaErrorCode`, e.g. `InvalidFormat`, `NotFound`, `HomeAssistantError`, `ServiceValidationError`; unknown codes are `Other(String)`). `translation: Option<Box<HaErrorTranslation>>` is `Some` when Home Assistant sent a `translation_key`: `HaErrorTranslation { domain: Option<String>, key: String, placeholders: Map<String, Value> }` (`#[non_exhaustive]`). Match with `..` to ignore it.
*   `Protocol(String)`, `Decode(serde_json::Error)`, `InvalidInput(String)`.

## 4. Main API: `HAClient`

### Lifecycle
*   `async fn connect(url: impl Into<String>, token: impl Into<String>) -> Result<HAClient>`: connects and authenticates; the first attempt is not retried. The URL must be `ws://` or `wss://` (`.../api/websocket`); `http(s)://` fails with `Error::InvalidInput`.
*   `async fn connect_with(config: ClientConfig) -> Result<HAClient>`.
*   `fn connect_lazy(config: ClientConfig) -> Result<HAClient>` (not async, call inside a Tokio runtime): does not wait for the first connection; only `Error::InvalidInput` for a bad URL. State is `Connecting` during the first attempt (made immediately), then `Reconnecting` with backoff. Requests fail with `NotConnected` until connected; subscriptions are accepted and start `Interrupted`. Wrong token: `AuthFailed`.
*   `async fn connected(&self) -> Result<()>`: waits for `Connected` (no internal timeout); `Err(Error::Auth(msg))` on `AuthFailed`, `Err(Error::Stopped)` on `Stopped`.
*   `ClientConfig::new(url, token)` with builder methods `.connect_timeout(Duration)` (10s), `.request_timeout(Duration)` (30s), `.ping_interval(Duration)` (30s), `.pong_timeout(Duration)` (10s, also bounds each write), `.reconnect_delay(min, max)` (1s, 60s), `.subscription_queue_size(usize)` (1024), `.slow_callback_warning(Duration)` (5s; `ZERO` disables), `.max_message_size(usize)` (64 MiB, also bounds frames), `.wait_for_connection(Duration)` (`ZERO`: requests wait this long for a down connection before `NotConnected`), `.tls_connector(TlsConnector)`. Durations are capped at one year (`Duration::MAX` is fine).
*   `async fn stop(&self)`: stops the client for all handles. Subscriptions receive `SubscriptionStatus::Closed`. Waits up to 5s for callbacks to handle queued events (not when called from inside a callback).
*   `async fn closed(&self) -> ConnectionState`: waits for `Stopped` or `AuthFailed`.
*   `fn connection_state(&self) -> ConnectionState`, `fn watch_connection_state(&self) -> tokio::sync::watch::Receiver<ConnectionState>`.
*   `ConnectionState`: `Connecting` (only after `connect_lazy`), `Connected`, `Reconnecting { attempt: u32 }`, `AuthFailed(String)`, `Stopped`; `is_terminal()`.

### State Operations
*   `async fn get_state(&self, entity_id: &str) -> Result<events::State>`: `Error::Ha { code: HaErrorCode::NotFound, .. }` if missing.
*   `async fn get_states(&self) -> Result<Vec<events::State>>`.
*   `async fn get_history(&self, entity_id: &str, start: DateTime<Utc>, end: DateTime<Utc>, significant_changes_only: bool) -> Result<Vec<HistoricalState>>` Unparsable entries are skipped with a warning.
*   `async fn get_state_at_time(&self, entity_id: &str, timestamp: DateTime<Utc>, lookback: chrono::TimeDelta) -> Result<events::State>`.

### Service Calls
*   `async fn call_service(&self, domain: impl Into<Domain>, service: impl Into<Service>, target: impl Into<Target>, data: impl Serialize) -> Result<()>`: enums or strings for domain/service. `data` must serialize to a JSON object (`()` for none), else `Error::InvalidInput`. Examples: `call_service("light", "turn_on", "light.kitchen", json!({"brightness": 120}))`, `call_service(Domain::HomeAssistant, Service::Reload, (), ())`, `call_service("light", "turn_off", Target::area("kitchen").and_entity("light.porch"), ())`.
*   `async fn command<R: DeserializeOwned>(&self, request: impl Serialize) -> Result<R>`: escape hatch for any one-shot WebSocket command without a dedicated method; decodes `result` into `R` (`Value` for raw). `request` must be a JSON object with a string `type` and no `id` (the client assigns ids), else `Error::InvalidInput`; also refused: `auth`, `unsubscribe_events` (use `unsubscribe`), `subscribe_*` and `render_template` (use `Source::raw`). Decode failure is `Error::Decode`, an HA error reply `Error::Ha`. `let cfg: Value = client.command(json!({"type": "get_config"})).await?;`
*   `async fn call_service_with_response<R: DeserializeOwned>(...same...) -> Result<R>`: decodes the service response (e.g. `weather.get_forecasts`); a missing response decodes from `null`. `let f: Value = client.call_service_with_response("weather", "get_forecasts", "weather.home", json!({"type": "daily"})).await?;`
*   `Target`: `Target::new()`, `entity(id)`, `entities(ids)`, `device(id)`, `area(id)`, `floor(id)`, `label(id)`, chained with `.and_entity/.and_device/.and_area/.and_floor/.and_label`, `is_empty()`. `()` is no target; `&str`, `String`, `[&str; N]`, `Vec<String>`, `Vec<&str>`, `&[&str]` convert to entity ids.

### Subscriptions
Every subscription delivers `Update<T>` (below), where `T` depends on the subscription. Callback bound: `F: Fn(HAClient, Update<T>) -> Fut + Send + Sync + 'static`, `Fut: Future<Output = ha_core_client::CallbackResult> + Send + 'static`, where `CallbackResult = Result<(), BoxError>` and `BoxError = Box<dyn std::error::Error + Send + Sync>`. A closure ending in `Ok(())` needs no annotation; `?` works on any `std::error::Error` and on `anyhow::Error`. To fail with a message: `return Err("reason".into())`. Errors returned by a callback are logged with their source chain.

Each `subscribe_*` returns `Result<Subscription>` after Home Assistant accepts it (an invalid configuration returns `Error::Ha`). While reconnecting (or if the connection drops before Home Assistant answers), subscribing succeeds: the subscription starts with `Interrupted` and is sent when the connection is back. If the caller stops waiting (cancelled future, timeout), the subscription is cancelled and its callback never runs.

*   `subscribe<T, F, Fut>(&self, source: Source<T>, cb: F) -> Result<Subscription>`: the general form; every method below is `subscribe(Source::..., cb)`.
*   `stream<T>(&self, source: Source<T>) -> Result<EventStream<T>>`: the same subscription as a `futures_util::Stream<Item = Update<T>>` (see "Streams" below). There are no `*_stream` variants: use `client.stream(Source::entity("x"))`.
*   `subscribe_entity(entity_id: &str, cb)` (`Update<StateChange>`): `StateChange` for that entity only (state and attribute changes, filtered server-side). Same as `subscribe_entity_with(entity_id, EntityChanges::Any, cb)`.
*   `subscribe_entity_with(entity_id: &str, changes: EntityChanges, cb)`: `EntityChanges::State` delivers only changes of the state string (attribute-only updates and queued changes that cancel out, `A → B → A`, are skipped); `EntityChanges::Any` delivers every recorded update. Neither sees a sensor re-reporting the same value with the same attributes: Home Assistant only bumps `last_reported` and does not send it over the WebSocket API (`state_reported` cannot be subscribed to), so "sensor silent for N minutes" logic must not rely on such reports (use `watch_silence` instead). Fails with `Error::Ha { code: HaErrorCode::NotFound, .. }` if the entity does not exist; use `subscribe_trigger(StateTrigger::new(id), cb)` for entities that may appear later.
*   `subscribe_state_changes(cb)` (`Update<StateChange>`): the `state_changed` bus event of **every** entity. No existence check, resync or merging; prefer `subscribe_entity` for single entities.
*   `subscribe_events(event_type: &str, cb)` (`Update<BusEvent>`): raw bus events of one type. `state_changed` and `call_service` are not special-cased here.
*   `subscribe_service_calls(cb)` (`Update<ServiceCall>`): all `call_service` events.
*   `subscribe_trigger(trigger: impl Into<triggers::Trigger>, cb)`, `subscribe_trigger_raw(trigger: serde_json::Value, cb)`: `TriggerFired` (`Update<TriggerFired>`). A raw trigger that is not a JSON object fails with `Error::InvalidInput`.
*   `subscribe_triggers<I: IntoIterator<Item: Into<Trigger>>>(triggers, cb)`: several triggers in one subscription (`"trigger": [..]`). Give each `.id("..")`; `TriggerFired::id()` says which fired. `Source::triggers(..)` is the same; `Source<TriggerFired>::variables(impl Serialize) -> Result<Self>` sets the `variables` of the request (`Error::InvalidInput` unless a JSON object).
*   `subscribe_template(template: &str, cb)`: Home Assistant renders the template and sends `TemplateUpdate::Rendered(Value)` (`Update<TemplateUpdate>`) right after subscribing, after every reconnect (following `Restored`, possibly unchanged) and whenever the result changes. Re-rendered when a referenced entity changes, and every minute if it uses `now()`. Results are typed (`2`, `true`, `"158 W"`). A syntax error fails the call with `Error::Ha`; a failing render arrives as `TemplateUpdate::RenderError { message, level: TemplateErrorLevel }` and the subscription stays.
*   `subscribe_template_as::<T: DeserializeOwned>(template: &str, cb)` (`Update<TemplateUpdate<T>>`): like `subscribe_template`, decoding each result into `T` (`f64`, `bool`, `String`, a struct, …); a result that does not fit arrives as `TemplateUpdate::DecodeError { result: Value, error: String }`.
*   `watch_silence(entity_id: &str, max_silence: std::time::Duration, cb)`: `Silence { entity_id, silent: bool }` (`Update<Silence>`): the current value right after subscribing and after every reconnect, then each flip. `silent` is `true` when the entity has not reported for longer than `max_silence`, counting reports of an unchanged value (`last_reported`), and for a missing entity. Built on `subscribe_template`; evaluated in Home Assistant on its clock, on entity changes and every minute, so flips can be up to a minute late. The way to implement "sensor silent for N minutes" without `get_states`. `Error::InvalidInput` for a malformed entity id.
*   `unsubscribe(&self, subscription: Subscription) -> Result<()>`: the callback then gets `SubscriptionStatus::Unsubscribed` as its last event.
*   `Subscription::id() -> u64`, which stays the same across reconnects. Dropping a `Subscription` does not unsubscribe. A `Subscription` belongs to the client that created it: `unsubscribe` on another client fails with `Error::InvalidInput` (ids are per client).
*   `Subscription::guard(self, &client) -> SubscriptionGuard`: unsubscribes on drop; `guard.subscription()`, `guard.unsubscribe().await`, `guard.into_inner()`. The guard holds a client handle.

#### `Source<T>`, streams and `WeakClient`
*   `subscribe_raw(request: serde_json::Value, cb)`: shortcut for `subscribe(Source::raw(request)?, cb)`; `Update<Value>` with each `event` payload unchanged. Same request rules as `command` (minus the `subscribe_*` ban); re-sent after reconnect.
*   `Source<T>` (crate root) describes one subscription and its item type `T`. Constructors: `Source::entity(id)`, `Source::entity_with(id, EntityChanges)`, `Source::state_changes()` (all `StateChange`); `Source::service_calls()` (`ServiceCall`); `Source::events(event_type)` (`BusEvent`); `Source::trigger(impl Into<Trigger>)`, `Source::trigger_raw(Value) -> Result<Source<TriggerFired>>` (`Error::InvalidInput` unless an object); `Source::template(t)` (`TemplateUpdate<Value>`), `Source::template_as::<T>(t)` (`TemplateUpdate<T>`); `Source::silence(id, max_silence) -> Result<Source<Silence>>`. `without_merging(self) -> Self` (any `Source<T>`): an entity subscription then keeps every change instead of merging them when the callback falls behind (the oldest are dropped past `subscription_queue_size`, reported as `Lagged`; with `EntityChanges::State` attribute-only changes are still skipped); no effect on other sources; `Source::raw(request: Value) -> Result<Source<Value>>` (any subscribe-style command, raw event payloads; `Error::InvalidInput` unless an object with string `type`, no `id`, not `auth`/`unsubscribe_events`).
*   `stream<T: Send + 'static>(&self, source: Source<T>) -> Result<EventStream<T>>`. `EventStream<T>` implements `futures_util::Stream<Item = Update<T>> + Unpin` and ends after the subscription's last status (`Closed`/`Unsubscribed`/`Rejected`). `subscription() -> &Subscription`; `async unsubscribe(self) -> Result<()>` waits for Home Assistant; dropping it unsubscribes. It holds an `HAClient` (keeps the client alive, like `SubscriptionGuard`). `stop()` does not wait for streams. Reconnect statuses, resync and queue merging behave exactly as for callbacks.
*   `HAClient::downgrade() -> WeakClient`; `WeakClient: Clone`, `upgrade() -> Option<HAClient>`. A callback that needs a client handle outside its own run should capture a `WeakClient`, not an `HAClient`, or dropping all user handles no longer stops the client.

```rust
use futures_util::StreamExt;
use ha_core_client::{Source, Update};

// Callback
client.subscribe(Source::entity("sensor.power"), |_client, update| async move {
    match update {
        Update::Event(change) => println!("{} -> {:?}", change.entity_id, change.new_state.map(|s| s.state)),
        Update::Status { status, .. } => println!("status: {status:?}"),
        _ => {}
    }
    Ok(())
}).await?;

// Stream
let mut stream = client.stream(Source::template_as::<f64>("{{ states('sensor.power') | float }}")).await?;
while let Some(update) = stream.next().await {
    if let Some(ha_core_client::events::TemplateUpdate::Rendered(watts)) = update.event() { println!("{watts} W"); }
}
```

### Sans-IO `Session` (no Tokio, e.g. WASM components)

With `default-features = false` there is no `HAClient`; use `ha_core_client::Session` (feature `runtime` provides `HAClient`). The host owns the WebSocket and drives the session synchronously, passing `now: std::time::Instant`:
- `Session::new(ClientConfig::for_session(token))`; each tick: `while let Some(a) = session.poll_action(now)` (`Action::Connect`: open the socket then `on_connected(now)`; `Action::Close`: drop it), `on_frame(now, &text)` per text frame, `on_disconnected(now, reason)` on socket failure/close/failed write, `tick(now)`, then `while let Some(f) = session.poll_transmit()` write each frame.
- `subscribe(now, Source) -> PendingSubscription<T>`; `poll()` gives `Some(Ok(SessionEvents<T>))`; `events.drain()`/`try_next()` yield `Update<T>` (data and `Update::Status`). `unsubscribe(now, &sub)`.
- Requests return `Reply<T>` to `poll()` later: `get_states`, `get_state`, `get_services`, `call_service`, `command` (raw JSON result).
- Not available there: `get_history`, `call_service_with_response`, domain helpers, `EventStream` (use `command`).

## 5. Automation Triggers

All fire `TriggerFired`. After a reconnect, triggers are re-created in Home Assistant, so a pending `for` countdown restarts.

Builders (preferred), passed to `subscribe_trigger`:

```rust
use ha_core_client::triggers::*;
client.subscribe_trigger(StateTrigger::new("binary_sensor.door").to(HaState::On).for_duration(Duration::from_secs(60)), cb).await?;
client.subscribe_trigger(NumericStateTrigger::new("sensor.temp").above(25.0), cb).await?;
client.subscribe_trigger(SunTrigger::new(SunEvent::Sunset).offset(TimeDelta::minutes(-30)), cb).await?;
client.subscribe_trigger(TimePatternTrigger::new().minutes("/5"), cb).await?;
// also TimeTrigger::new("07:30").and_at("input_datetime.alarm"), TemplateTrigger::new(t).for_duration(d), ZoneTrigger::new(id, "zone.home", ZoneEvent::Enter)
client.subscribe_triggers([StateTrigger::new("a.x").id("x"), StateTrigger::new("a.y").id("y")], cb).await?; // fired.id()
```

*   All platform structs and `Trigger` are `#[non_exhaustive]`: use `new` + builders. Every platform has `.id(..)`.
*   `StateTrigger`: `new(id)`, `entities(iter)`, `and_entity(id)` (entity_id is a list); `to(s)`, `to_any_of(iter)`, `to_any_state()` (sends `to: null`: any state change, not attribute-only changes); same for `from*`; `not_to(iter)`, `not_from(iter)`; `for_duration`, `attribute`.
*   `NumericStateTrigger`: `new`/`entities`/`and_entity`; `above(f64)`, `above_entity(id)`, `below(f64)`, `below_entity(id)`, `value_template`, `for_duration`, `attribute`.
*   `TimeTrigger`: `new(at)`, `and_at(at)`; `at` is `HH:MM[:SS]`, an `input_datetime` or a timestamp-sensor entity id.
*   `TimePatternTrigger`: `new()` + `hours(p)`, `minutes(p)`, `seconds(p)` (`"/5"`, `"30"`, `"*"`).
*   Public fields can be read: `StateTrigger::to`/`from` are `Option<StateMatch>` (`Any`, `One(HaState)`, `AnyOf(Vec<HaState>)`), `NumericStateTrigger::above`/`below` are `Option<Threshold>` (`Value(f64)`, `Entity(String)`).
*   Other platforms: `EventTrigger::new(type)`/`any_of(iter)` + `event_data(impl Serialize) -> Result<Self>`; `HomeAssistantTrigger::new(HomeAssistantEvent::Start|Shutdown)`; `MqttTrigger::new(topic)` + `payload`, `value_template`, `encoding`, `qos`; `WebhookTrigger::new(id)` + `allowed_methods([WebhookMethod::..])`, `local_only(bool)`; `CalendarTrigger::new(entity, CalendarEvent::Start|End)` + signed `offset(TimeDelta)`; `TagTrigger::new(tag)` + `and_tag`, `device`; `ConversationTrigger::new(sentence)` + `and_command`.
*   Event `context` filters, `geo_location`, `device`, `persistent_notification` and `time` `weekday` are not modelled: use `subscribe_trigger_raw(serde_json::json!({"trigger": "mqtt", "topic": "x"}), cb)` (see `TRIGGERS_OVERVIEW.md`).

## 6. Domain Helpers (Recommended)

Use these instead of `call_service` for cleaner code. All return `Result<()>`. Wherever `id` appears below, the parameter is `impl Into<Target>`: `"light.x"`, a list of ids, `Target::area("kitchen")`, …

*   **General**: `turn_on(target)`, `turn_off(target)`, `toggle(target)`. Entity ids that all share one domain call `<domain>.<service>`; areas/devices/floors/labels, mixed domains or an empty target call `homeassistant.<service>`. An invalid entity id is `Error::InvalidInput`.
*   **Climate**: `set_hvac_mode(id, HvacMode)`, `set_temperature(id, f64)`, `set_temperature_range(id, low, high)` (`heat_cool`), `set_fan_mode(id, FanMode)`, `set_preset_mode(id, PresetMode)`, `set_swing_mode(id, SwingMode)`.
*   **Cover**: `open_cover(id)`, `close_cover(id)`, `stop_cover(id)`, `set_cover_position(id, u8)`, `set_cover_tilt_position(id, u8)`.
*   **Alarm Control Panel**: `alarm_disarm(id, code: Option<&str>)`, `alarm_arm_home/away/night/vacation(id, code)`.
*   **Lock**: `lock(id, code: Option<&str>)`, `unlock(id, code)`.
*   **Input**: `set_number(id, f64)`, `set_text(id, &str)`, `select_option(id, &str)`. The service domain comes from the entity id (`number.x` → `number.set_value`); a bare name means `input_*` (`"x"` → `input_number.x`). Entity ids of different domains fail with `Error::InvalidInput`; a target without entity ids (an area) uses the `input_*` domain.

## 7. Important Types & Enums

### `ha_core_client::state`
*   `Domain`, `Service`: known variants (`Domain::Light`, `Domain::HomeAssistant`, …, `Service::TurnOn`, `Service::Reload`, …) plus `Other(String)`. `From<&str>`, `as_str()` and `Display` give the wire string, `Domain::of_entity("light.x")`. Equality uses the wire string, so `Domain::Other("light".into()) == Domain::Light`.
*   `HaState`: `On`, `Off`, `Unavailable`, `Unknown`, `Home`, `NotHome`, `Alarm(AlarmState)`, `Climate(HvacMode)`, `Cover(CoverState)`, `Lock(LockState)`, `Number(f64)`, `String(String)`. `HaState::from_string(&str)` (domain-agnostic; `from_value(&Value)` for JSON), `HaState::parse_for_domain(&Domain, &str)` (resolves e.g. `open` for lock vs cover); `State::ha_state()` uses the entity's domain. Parsing is exact and case-sensitive like Home Assistant states (`"on"` is `On`, `"On"` is `String("On")`), so a parsed `HaState` (and `.to("Home")` in a trigger) prints back exactly the original string. A climate entity's `"off"` is `Climate(HvacMode::Off)`, not `Off`. Serializes as the plain state string. `Number(f64)` only when the string prints back unchanged (`"21"`, `"21.5"`); `"21.0"`, `"1e3"`, `"nan"` stay `String`, so for numeric logic parse `state.state` with `.parse::<f64>()`.
*   `climate::{HvacMode, HvacAction, FanMode, SwingMode, PresetMode}`. `HvacMode` is closed (Home Assistant's fixed set); the others have `Other(String)`.
*   `device_class::DeviceClass`, `state_class::StateClass`, `common::UnitOfMeasurement`: all with `Other(String)`.
*   All string enums are `#[non_exhaustive]`: `match` needs a wildcard arm.

### `ha_core_client::events`
*   `Update<T>` (`#[non_exhaustive]`, re-exported at the crate root): `Event(T)` (a data event) or `Status { subscription: Subscription, status: SubscriptionStatus }`. Helpers `into_event() -> Option<T>`, `event() -> Option<&T>`, `status() -> Option<&SubscriptionStatus>`. A data event that does not fit the subscription's type (e.g. a malformed payload) is logged with `warn!` and skipped.
*   Item types by subscription (`events` module):
    *   `StateChange { entity_id: String, old_state: Option<State>, new_state: Option<State> }` (`subscribe_entity[_with]`, `subscribe_state_changes`): `old_state` is `None` if the entity was added, `new_state` is `None` if it was removed. `old_state.state` may equal `new_state.state` (attribute-only change, or merged changes). Helpers: `is_state_change()` (state string differs, or added/removed), `added()`, `removed()`.
    *   `ServiceCall { domain: Domain, service: Service, service_data: Value, context: Option<Context> }` (`subscribe_service_calls`).
    *   `BusEvent { event_type: String, data: Value, origin: Option<String>, time_fired: Option<DateTime<Utc>>, context: Option<Context> }` (`subscribe_events`) with `data_as::<T: DeserializeOwned>() -> Result<T>` (`Error::Decode`). `time_fired` is `None` if absent or unparsable.
    *   `TriggerFired { trigger: Value, context: Option<Context> }` (`subscribe_trigger[_raw]`) with `platform()`, `description()`, `entity_id()`, `from_state()`, `to_state()`.
    *   `TemplateUpdate<T = Value>` (`#[non_exhaustive]`; `subscribe_template`, `subscribe_template_as`): `Rendered(T)`, `RenderError { message: String, level: TemplateErrorLevel }` (`Error`/`Warning`/`Other`; the subscription stays active), `DecodeError { result: Value, error: String }`.
    *   `Silence { entity_id: String, silent: bool }` (`watch_silence`).
*   `SubscriptionStatus` (`#[non_exhaustive]`; the `status` of `Update::Status`): lifecycle of the subscription itself, delivered in order with its data:
    *   `Interrupted`: the connection was lost. No events until `Restored`, so treat derived data as unknown.
    *   `Restored`: active again. For `subscribe_entity`, a change missed while disconnected arrives just before as one `StateChange` (last known → current; `new_state: None` if the entity was removed).
    *   `Lagged { dropped }`: the callback fell behind. For `subscribe_entity`, `dropped` state changes were merged into the next `StateChange` (unless `without_merging()`: then the oldest are discarded, as below); for other subscriptions, the `dropped` oldest events were discarded (queue limit `subscription_queue_size`). Statuses are never dropped.
    *   `Rejected { code: HaErrorCode, message }`: Home Assistant refused it when restoring after a reconnect. Removed, no more events.
    *   `Closed`: the client stopped. No more events.
    *   `Unsubscribed`: cancelled by `unsubscribe()` or a dropped `SubscriptionGuard`/`EventStream`. No more events.
    *   `subscription` can be passed to `client.unsubscribe(...)`, also while offline.
*   `Context { id: String, parent_id: Option<String>, user_id: Option<String> }` (crate root, `#[non_exhaustive]`): who or what caused a change. `is_user()` (`user_id` set: a user's action) and `is_automated()` (`parent_id` set: caused by an automation or script reacting to another event). Parsing never fails; unknown shapes give `Context::default()`.
*   `State { entity_id, state: String, attributes: AttributeSet, last_changed, last_updated, last_reported: Option<_>, context: Context }` with `ha_state() -> HaState`, `domain() -> Option<Domain>` and `last_seen() -> DateTime<Utc>` (`last_reported`, or `last_updated` when it is `None`, as in `get_state` and history results).
*   `AttributeSet` (`#[non_exhaustive]`, match with a wildcard arm), typed by the entity's domain; every typed struct is `#[non_exhaustive]`, has `friendly_name`, `icon` and `rest: serde_json::Map` (all other keys), and every field is `Option` unless noted. If a domain's attributes do not parse, the entity is `Generic` (never an error). Variants (all `Box<..>` except `Generic`), accessor `as_<name>() -> Option<&T>`:
    *   `Climate(ClimateAttributes)` (`climate`; no `icon` field, it is in `rest`), `as_climate`: see below.
    *   `Sensor(SensorAttributes)` (`sensor`), `as_sensor`: `unit_of_measurement: UnitOfMeasurement`, `device_class: DeviceClass`, `state_class: StateClass`, `options: Vec<String>` (enum sensors), `last_reset: DateTime<Utc>`.
    *   `BinarySensor(BinarySensorAttributes)` (`binary_sensor`), `as_binary_sensor`: `device_class`.
    *   `Light(LightAttributes)` (`light`), `as_light`: `color_mode: ColorMode`, `supported_color_modes: Vec<ColorMode>`, `brightness: u8`, `color_temp_kelvin`, `min_color_temp_kelvin`, `max_color_temp_kelvin: u32`, `hs_color`, `xy_color: (f64, f64)`, `rgb_color: (u8, u8, u8)`, `rgbw_color`, `rgbww_color` (4 and 5 `u8`), `effect: String`, `effect_list: Vec<String>`, `supported_features: u32`.
    *   `Cover(CoverAttributes)` (`cover`), `as_cover`: `current_position: u8`, `current_tilt_position: u8`, `device_class`, `supported_features`.
    *   `Fan(FanAttributes)` (`fan`), `as_fan`: `percentage: u8`, `percentage_step: f64`, `preset_mode: String`, `preset_modes: Vec<String>`, `oscillating: bool`, `direction: FanDirection`, `supported_features`.
    *   `MediaPlayer(MediaPlayerAttributes)` (`media_player`), `as_media_player`: `volume_level: f64`, `is_volume_muted: bool`, `media_content_id`, `media_content_type`, `media_duration: f64`, `media_position: f64`, `media_position_updated_at: DateTime<Utc>`, `media_title`, `media_artist`, `media_album_name`, `app_name`, `source`, `sound_mode: String`, `source_list`, `sound_mode_list: Vec<String>`, `shuffle: bool`, `repeat: RepeatMode`, `entity_picture`, `supported_features`.
    *   `Number(NumberAttributes)` (`number`, `input_number`), `as_number`: `min`, `max`, `step: f64`, `mode: NumberMode`, `unit_of_measurement`, `editable: bool`.
    *   `Select(SelectAttributes)` (`select`, `input_select`), `as_select`: `options: Vec<String>` (not an `Option`), `editable`.
    *   `Update(UpdateAttributes)` (`update`), `as_update`: `installed_version`, `latest_version`, `skipped_version`, `release_summary`, `release_url`, `title: String`, `in_progress: bool` (old Home Assistant sent a percentage: a non-zero number reads as `true`), `update_percentage: f64`, `auto_update: bool`, `display_precision: u32`, `device_class`, `entity_picture`, `supported_features`.
    *   `Weather(WeatherAttributes)` (`weather`), `as_weather`: `wind_bearing: Option<WindBearing>` (`Degrees(f64)` or `Compass(String)` like `"NNE"`); `f64` (JSON ints included): `temperature`, `apparent_temperature`, `dew_point`, `humidity`, `pressure`, `wind_speed`, `wind_gust_speed`, `cloud_coverage`, `uv_index`, `visibility`, `ozone`; `UnitOfMeasurement`: `temperature_unit`, `pressure_unit`, `wind_speed_unit`, `visibility_unit`, `precipitation_unit`; `attribution`, `supported_features`.
    *   `Sun(SunAttributes)` (`sun`), `as_sun`: `next_dawn`, `next_dusk`, `next_midnight`, `next_noon`, `next_rising`, `next_setting: DateTime<Utc>`, `elevation`, `azimuth: f64`, `rising: bool`.
    *   `Person(PersonAttributes)` (`person`), `as_person`: `latitude`, `longitude`, `gps_accuracy: f64`, `source`, `user_id`, `id`, `device_trackers: Vec<String>` and `in_zones: Vec<String>` (not `Option`, empty by default), `editable`, `entity_picture`.
    *   `DeviceTracker(DeviceTrackerAttributes)` (`device_tracker`), `as_device_tracker`: `latitude`, `longitude`, `gps_accuracy`, `altitude`, `course`, `speed`, `vertical_accuracy`, `battery_level: f64`, `source_type: SourceType`, `in_zones: Vec<String>`.
    *   `Zone(ZoneAttributes)` (`zone`), `as_zone`: `latitude`, `longitude`, `radius: f64`, `passive: bool`, `persons: Vec<String>`, `editable`.
    *   `Automation(AutomationAttributes)` (`automation`), `as_automation`: `id: String`, `last_triggered: DateTime<Utc>`, `mode: RunMode`, `current: u32`, `max: u32`.
    *   `Generic(GenericAttributes { friendly_name, unit_of_measurement, device_class, state_class, rest })` (everything else), `as_generic`.
    *   Attribute enums in `ha_core_client::state::{light, fan, media_player, number, device_tracker, automation}`: `ColorMode`, `FanDirection`, `RepeatMode`, `NumberMode`, `SourceType`, `RunMode` (each with `Other(String)`).
    *   Methods: `friendly_name()`, `icon()`, `get(key) -> Option<Value>` (any attribute as JSON, `None` if missing or null), `get_as::<T>(key) -> Option<T>` (decoded, `None` if missing or another type). Example: `state.attributes.as_light().and_then(|l| l.brightness)`.
*   `ClimateAttributes { friendly_name, hvac_modes: Option<Vec<HvacMode>> (unknown modes skipped), hvac_action: Option<HvacAction>, current_temperature, current_humidity, temperature, target_temp_low, target_temp_high, target_temp_step, min_temp, max_temp, fan_mode, fan_modes, preset_mode, preset_modes, swing_mode, swing_modes, rest }`. The current HVAC mode is the state (`State::ha_state()`). Structs are `#[non_exhaustive]`: read fields, do not build literals.
*   `HistoricalState { entity_id, state, attributes: AttributeSet, last_changed, last_updated }`; `State::from(historical)`.

## 8. Implementation Pattern

```rust
use ha_core_client::{HAClient, Update, events::StateChange, state::HaState};

async fn on_motion(client: HAClient, update: Update<StateChange>) -> ha_core_client::CallbackResult {
    if let Update::Event(StateChange { new_state: Some(state), .. }) = update {
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
*   **Stale data**: react to `Update::Status` with `SubscriptionStatus::Interrupted`/`Restored` instead of polling `connection_state()`.
*   **Retries**: `NotConnected` means nothing was sent, so retrying is safe (or set `wait_for_connection` to let the client wait for the connection). `ConnectionLost`/`Timeout` mean the outcome is unknown, so check the state before retrying non-idempotent calls.
