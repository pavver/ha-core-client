# Changelog

## Unreleased

### Breaking

*   New `ConnectionState::Connecting` variant (the enum is `non_exhaustive`, so matches that already have a `_` arm are unaffected; exhaustive-in-spirit matches should handle it). It is only seen after `connect_lazy`. Nothing else changes for existing code; `Runner` internals are not public.
*   `call_service(domain, service, target, data)`: `target` is `impl Into<Target>` and `data` is `impl Serialize`. Migrate `call_service(d, s, Some("light.x"), None)` to `call_service(d, s, "light.x", ())`, `Some(json!(..))` data to plain `json!(..)`, and a `None` target to `()`. Data must serialize to a JSON object (or `()`/`null` for none), otherwise `Error::InvalidInput`.
*   `call_service_with_response` is generic: `call_service_with_response::<R>(..) -> Result<R>` decodes the `response` field (`Value` gives the old behaviour; decode failures are `Error::Decode`).
*   The `target` sent to Home Assistant now lists entity ids (`{"entity_id": ["light.x"]}`) instead of a bare string.
*   `Error::Ha { code }` and `SubscriptionStatus::Rejected { code }` are `HaErrorCode` instead of `String`. Compare with `code == HaErrorCode::NotFound`; its `Debug` output is now the variant name.
*   `ServiceCalledEvent::domain` and `service` are `Domain` and `Service` instead of `String`.
*   Subscription callbacks no longer receive one big `events::Event` enum: each subscription kind delivers its own type, wrapped in `Update<T>` (`Update::Event(T)` or `Update::Status { subscription, status }`). `events::Event` is gone. Callback signature: `Fn(HAClient, Update<T>) -> Fut`. Migration:

    | Before | After |
    |---|---|
    | `Event::StateChanged { entity_id, old_state, new_state }` | `Update::Event(StateChange { entity_id, old_state, new_state })` |
    | `Event::Status { subscription, status }` | `Update::Status { subscription, status }` |
    | `Event::TemplateRendered { result }` | `Update::Event(TemplateUpdate::Rendered(result))` |
    | `Event::TemplateError { message, level: String }` | `Update::Event(TemplateUpdate::RenderError { message, level: TemplateErrorLevel })` |
    | `Event::Silence { entity_id, silent }` | `Update::Event(Silence { entity_id, silent })` |
    | `Event::Generic { event_type, data }` | `Update::Event(BusEvent { event_type, data, .. })` |
    | `Event::ServiceCalled(ServiceCalledEvent)` | `Update::Event(ServiceCall)` (renamed) |
    | `Event::TriggerFired(TriggerFiredEvent)` | `Update::Event(TriggerFired)` (renamed) |
    | `subscribe_events("state_changed", ..)` | `subscribe_state_changes(..)` (`subscribe_events("state_changed", ..)` now delivers raw `BusEvent`s) |
    | `subscribe_events("call_service", ..)` | `subscribe_service_calls(..)` |

*   `subscribe_events` always delivers `BusEvent`; `state_changed` and `call_service` are no longer special-cased.
*   A `watch_silence` template render error (and an entity-subscription payload that cannot be parsed) is logged and skipped instead of being delivered as another event type.
*   `AttributeSet` has new variants (`Sensor`, `BinarySensor`, `Light`, `Cover`, `Fan`, `MediaPlayer`, `Number`, `Select`, `Update`, `Weather`, `Sun`, `Person`, `DeviceTracker`, `Zone`, `Automation`). Code matching only `Climate`/`Generic` must add a wildcard arm, and entities of these domains no longer come as `Generic` (attributes that do not match the schema still do). Typed fields moved out of `rest` for these entities.

### Fixed

*   `HaState` parsing ignored case and printed the canonical form, so `StateTrigger::new("input_select.mode").to("Home")` was sent as `"to": "home"` and never fired (states are case-sensitive in Home Assistant). Parsing is now exact: `"on"` is `HaState::On`, `"On"` is `String("On")`, and every `HaState` prints back the string it was parsed from. Behaviour change: `HaState::from_string("ON")` (and `State::ha_state()` of a state `"ON"`) is no longer `HaState::On`.
*   `watch_silence` failed with a template syntax error for entity ids whose object id starts with a digit (`sensor.1st_floor`). The template now uses `states.sensor['1st_floor']`.
*   `get_history` failed as a whole when a single history entry could not be parsed. Such entries are now skipped with a warning, like in `get_states`.
*   The reconnect backoff started over after every successful connect, so a connection that Home Assistant (or a proxy) dropped right after `auth_ok` was retried at the shortest delay forever. It now starts over only after a session that stayed up for the longest backoff delay (at most a minute); `ConnectionState::Reconnecting { attempt }` keeps counting until then.
*   `subscribe_entity` (subscribe, then read the state) gave each step its own `request_timeout`, so it could take up to twice as long, and the caller's own timeout fired first with `Timeout` while the subscription was still being set up. Both steps now share one deadline.
*   The request timeout did not cover waiting for room in the command queue, so a call could wait indefinitely while the client task was busy.
*   `subscribe_entity` did not report an entity that was removed and then added back while disconnected: after a removal the last known state was lost, so the re-read had nothing to compare with. The callback now gets `StateChanged { old_state: None, new_state: Some(..) }` before `Restored`.
*   A re-subscription that Home Assistant did not confirm within `request_timeout` ended the connection, interrupting every subscription; one that never got an answer kept the client reconnecting forever. It is now sent again on the same connection, and the others stay active.
*   The reconnect jitter came from the clock's nanoseconds, which on platforms with a coarse clock gave only a few distinct delays. It now uses a random value.
*   `AttributeSet::get` serialized all attributes on every call; it now looks up the one key.
*   Large results (`get_states`, history) no longer block event delivery and pings while being parsed: the runner keeps them as raw JSON and the caller decodes them.

### Added

*   `HAClient::connect_lazy(config)`: returns at once (inside a Tokio runtime) and connects in the background, first attempt immediately in `ConnectionState::Connecting`, then with backoff. Requests fail with `NotConnected` and subscriptions start `Interrupted` until connected; a rejected token ends in `AuthFailed`.
*   `HAClient::connected()`: waits until connected; `Err(Error::Auth)` / `Err(Error::Stopped)` on a terminal state.
*   `ClientConfig::wait_for_connection(Duration)` (default `ZERO`): request-style calls wait up to this long for the connection instead of failing at once with `NotConnected` (nothing is sent while waiting).
*   `ClientConfig::tls_connector(TlsConnector)` and the re-export `TlsConnector` (`tokio_tungstenite::Connector`): custom TLS setup, e.g. a private CA or a self-signed certificate.
*   Typed attribute structs per domain (`SensorAttributes`, `BinarySensorAttributes`, `LightAttributes`, `CoverAttributes`, `FanAttributes`, `MediaPlayerAttributes`, `NumberAttributes`, `SelectAttributes`, `UpdateAttributes`, `WeatherAttributes`, `SunAttributes`, `PersonAttributes`, `DeviceTrackerAttributes`, `ZoneAttributes`, `AutomationAttributes`) and the enums `ColorMode`, `FanDirection`, `RepeatMode`, `NumberMode`, `SourceType`, `RunMode`. `AttributeSet` gained `icon()`, `get_as::<T>(key)` and one `as_*` accessor per variant.
*   `Target`: what a service call acts on (entities, devices, areas, floors, labels), with `From` impls for entity ids.
*   `HaErrorCode`: typed Home Assistant error codes with an `Other(String)` fallback.
*   `Source<T>` (`Source::entity`, `entity_with`, `state_changes`, `service_calls`, `events`, `trigger`, `trigger_raw`, `template`, `template_as`, `silence`) and `HAClient::subscribe(source, callback)`: the general form of the `subscribe_*` methods.
*   `HAClient::stream(source) -> EventStream<T>`: consume any subscription as a `futures_util::Stream<Item = Update<T>>` (same statuses, resync and ordering as callbacks; dropping the stream unsubscribes).
*   `subscribe_state_changes` (the `state_changed` bus event as typed `StateChange`s), `subscribe_template_as::<T>` (`TemplateUpdate<T>` with `DecodeError`), `StateChange::{is_state_change, added, removed}`, `BusEvent::data_as`, `TemplateErrorLevel`.
*   `HAClient::downgrade() -> WeakClient` (`upgrade() -> Option<HAClient>`), for callbacks that need a client handle without keeping the client alive.

### Changed

*   `connect`/`connect_with` fail with `Error::InvalidInput` for a URL that is not `ws://` or `wss://`, with a hint for the common `http://host:8123` mistake (was an opaque `Error::Connection`).
*   `subscribe_trigger_raw` fails with `Error::InvalidInput` for a trigger that is not a JSON object, instead of sending it to Home Assistant.
*   A `Subscription` belongs to the client that created it. Since ids are per client, passing one to another client's `unsubscribe` (or `guard`) cancelled an unrelated subscription; it now fails with `Error::InvalidInput`.

## 0.3.0

Fixes from a code review of the runner and the typed model, and removal of legacy API. Breaking: the callback result type, the positional trigger helpers, the attribute types (see the migration table).

### Fixed

*   A subscription whose caller stopped waiting (its own timeout, a dropped future, or the client-side timeout firing first, e.g. `subscribe_entity` with its two round trips and a short `request_timeout`) stayed active forever: its callback kept running and it could never be unsubscribed. It is now removed and unsubscribed in Home Assistant, and its callback never runs.
*   The set of ignored server-side subscription ids grew with every `get_state`, `subscribe_entity` and `unsubscribe` until the next reconnect (a slow memory leak in long-running processes). Ids are now forgotten once Home Assistant confirms the unsubscribe.
*   `HaState::Number` lost the original string (`"21.0"` became `"21"`), so a trigger built from a parsed state never fired. A state is now `Number` only if it prints back unchanged; otherwise it stays `String`. `"nan"`/`"inf"` are no longer numbers.
*   `set_number`, `set_text` and `select_option` called the `input_*` service even for `number.*`, `text.*` and `select.*` entities. The domain now comes from the entity id; a bare object id still means `input_*`.
*   After a reconnect, live events of `subscribe_entity` could arrive before `Restored`, and a change could be missed (`B→C` delivered without `A→B`). Live events are now held until the state re-read: the callback gets one `StateChanged` from the last known state, then `Restored`, then any newer events. The same update seen by both the re-read and the trigger is delivered only once.
*   A subscribe that was in flight when the connection dropped failed with `ConnectionLost`, while one made a moment later (offline) succeeded. Both now succeed: the subscription starts as `Interrupted` and is restored on reconnect.
*   A steady stream of frames could starve commands and the housekeeping tick (timeouts, ping) in the connection loop; the loop no longer prefers incoming frames.
*   Attribute typing used a key heuristic: a `water_heater` with `current_temperature` got `ClimateAttributes`. In `State` and `HistoricalState` the entity's domain now decides (`Climate` only for `climate.*`).
*   `Domain::Other("light".into())` was not equal to `Domain::Light` (same for every `string_enum!` type). Equality and hashing now use the wire string.
*   `stop()` during the re-subscription of many subscriptions waited until all were sent.
*   An event that Home Assistant sent before the subscription's `result` was taken for an unknown subscription, which was then cancelled. It is now delivered after the confirmation.
*   Messages larger than 16 MiB (e.g. `get_states` on very large installations) were rejected by the WebSocket frame limit.

### Added

*   `subscribe_entity_with(entity_id, EntityChanges, cb)`: `EntityChanges::State` delivers only changes of the state (attribute-only updates are filtered in Home Assistant with `to: null`, and on the client, including queued changes that cancel out); `EntityChanges::Any` is the `subscribe_entity` behaviour. A sensor re-reporting an unchanged value is not observable over the WebSocket API (Home Assistant refuses `state_reported` subscriptions without a server-side filter), so neither mode sees it.
*   `subscribe_template(template, cb)`: a `render_template` subscription, `Event::TemplateRendered { result }` / `Event::TemplateError { message, level }`.
*   `watch_silence(entity_id, max_silence, cb)`: `Event::Silence { entity_id, silent }` when an entity stops reporting (unchanged reports count, via `last_reported`) and when it resumes. Evaluated in Home Assistant every minute, so no `get_states` polling.
*   `State::last_seen()`: `last_reported`, or `last_updated` where it is missing.
*   `SubscriptionStatus::Unsubscribed`, delivered after `unsubscribe()` as the last event.
*   `SubscriptionGuard`: `subscription.guard(&client)` unsubscribes on drop; `unsubscribe()` and `into_inner()`.
*   Trigger builders: `StateTrigger::new(id).to(..).from(..).for_duration(..).attribute(..)`, `NumericStateTrigger::new(id).above(..).below(..)`, `SunTrigger::new(event).offset(..)`, `TimeTrigger::new(at)`, `TimePatternTrigger::new().minutes("/5")`, `TemplateTrigger::new(t)`, `ZoneTrigger::new(..)`; `subscribe_trigger` accepts any of them (`impl Into<Trigger>`).
*   `ClientConfig::max_message_size` (default 64 MiB, applies to frames too).
*   `set_temperature_range(id, low, high)` for `heat_cool` mode.
*   Callback errors are logged with their source chain.

### Changed

*   **Home Assistant 2024.10 or newer is required.** Triggers are sent with the `trigger` key instead of the deprecated `platform`. `connect` logs a warning for older versions.
*   Callbacks return `ha_core_client::CallbackResult` = `Result<(), BoxError>` (`Box<dyn std::error::Error + Send + Sync>`) instead of `anyhow::Result<()>`. The library no longer depends on `anyhow`.
*   `stop()` also waits (up to 5s) for callbacks to handle the events already queued, ending with `Closed`. Called from inside a callback it does not wait for callbacks.
*   Removed the positional trigger helpers (`subscribe_state_trigger`, `subscribe_numeric_state_trigger`, `subscribe_sun_trigger`, `subscribe_time_trigger`, `subscribe_time_pattern_trigger`, `subscribe_template_trigger`, `subscribe_zone_trigger`): use `subscribe_trigger` with a builder.
*   `ClimateAttributes`: `friendly_name` is a field (it was only in `rest`), new `hvac_action` (`HvacAction`), `current_humidity`, `target_temp_low`/`high`/`step`, `fan_mode`, `preset_mode`, `swing_mode`; `rest` is a `serde_json::Map`. An unknown value in `hvac_modes` is skipped instead of turning the whole set into `Generic`. `AttributeSet::Climate` holds a `Box`. `AttributeSet::get(key)` reads any attribute as JSON.
*   History entries use `AttributeSet` too (they carry the same attributes as live states): `HistoricalAttributeSet`, `HistoricalClimateAttributes` and `HistoricalGenericAttributes` are removed.
*   `AttributeSet`, `ClimateAttributes` and `GenericAttributes` are `#[non_exhaustive]`, so attributes can be added later without a breaking release.
*   `unsubscribe()` of a request in flight when the connection drops succeeds (the server-side subscription ended with the connection) instead of failing with `ConnectionLost`.

### Migration from 0.2

| 0.2 | 0.3 |
|---|---|
| callback `-> anyhow::Result<()>` (named `async fn`) | `-> ha_core_client::CallbackResult`. Closures ending in `Ok(())` compile unchanged; `?` works on any `std::error::Error` and on `anyhow::Error`. |
| `anyhow::bail!(..)` / `Err(anyhow!(..))` in a callback | `return Err("..".into())`, or `Err(anyhow!(..))?` |
| `HaState::Number(21.0)` from `"21.0"` | `HaState::String("21.0")`; parse `state.state` as `f64` for numeric comparisons |
| `match` on `SubscriptionStatus` | handle (or wildcard) the new `Unsubscribed` |
| `AttributeSet::Climate` for non-`climate` entities with `current_temperature` | `AttributeSet::Generic` |
| Home Assistant older than 2024.10 | not supported (triggers use the `trigger` key) |
| `subscribe_trigger_raw(json!({"platform": ..}))` | still works; prefer `"trigger"` |
| `subscribe_state_trigger(id, to, from, for, cb)` | `subscribe_trigger(StateTrigger::new(id).to(..).from(..).for_duration(..), cb)` |
| `subscribe_numeric_state_trigger(id, above, below, for, cb)` | `subscribe_trigger(NumericStateTrigger::new(id).above(..).below(..).for_duration(..), cb)` |
| `subscribe_sun_trigger(event, offset, cb)` | `subscribe_trigger(SunTrigger::new(event).offset(..), cb)` |
| `subscribe_time_trigger(at, cb)` | `subscribe_trigger(TimeTrigger::new(at), cb)` |
| `subscribe_time_pattern_trigger(h, m, s, cb)` | `subscribe_trigger(TimePatternTrigger::new().hours(..).minutes(..).seconds(..), cb)` |
| `subscribe_template_trigger(t, for, cb)` | `subscribe_trigger(TemplateTrigger::new(t).for_duration(..), cb)` |
| `subscribe_zone_trigger(id, zone, event, cb)` | `subscribe_trigger(ZoneTrigger::new(id, zone, event), cb)` |
| `climate.rest.get("friendly_name")` | `climate.friendly_name` (or `attributes.friendly_name()`) |
| `ClimateAttributes.rest: HashMap<String, Value>` | `serde_json::Map<String, Value>` |
| `AttributeSet::Climate(c)` holds `ClimateAttributes` | holds `Box<ClimateAttributes>` (field access is unchanged) |
| `HistoricalState.attributes: HistoricalAttributeSet` | `AttributeSet` |
| building `ClimateAttributes { .. }` / `GenericAttributes { .. }` literals | not possible (`#[non_exhaustive]`): deserialize, or use `Default` and set fields |
| `match` on `AttributeSet` | add a wildcard arm |

## 0.2.0

A production-hardening release. The public API changed; see the migration guide below.

### Fixed

*   The client no longer dies when Home Assistant restarts: a server `Close` frame now triggers a reconnect instead of ending the background task.
*   Subscriptions are restored after a reconnect with fresh, strictly increasing ids, in creation order. Previously they reused old ids in random order, and Home Assistant rejected some of them (`id_reuse`).
*   State triggers sent `"to": null` for unit states (`HaState::On`, …) and capitalised lock/alarm states (`"Locked"`). `HaState` now serializes to the plain state string.
*   Trigger durations (`for`) and sun offsets were sent as `"1m 30s"`, which Home Assistant rejects. They are now sent as `HH:MM:SS`, and sun offsets can be negative.
*   `get_state` failed for every entity last changed by a user or an automation (context sent as an object), and hung forever if Home Assistant rejected the request.
*   `Event::TriggerFired` was never produced: the payload format did not match Home Assistant's.
*   `Domain::HomeAssistant` was sent as `home_assistant` instead of `homeassistant`.
*   `set_preset_mode` sent `"Eco"`/`"Custom"` instead of the preset name.
*   One unknown attribute value (e.g. a fan mode like `quiet`, a new device class) made `get_states` fail for the whole installation.
*   `UnitOfMeasurement` mapped `kW` to `KilowattHours`.
*   `State::ha_state()` misread ambiguous states (a lock's `open` became `Cover(Open)`); it now uses the entity's domain. New `HaState::parse_for_domain`.
*   `stop()` failed in-flight requests with `ConnectionLost`; they now fail with `Stopped`.
*   The access token appeared in trace logs.
*   A very large timeout (e.g. `Duration::MAX`) panicked the background task; durations are now capped at one year. If the task ever ends unexpectedly, the state becomes `Stopped` so `stop()`/`closed()` don't hang.
*   A write blocked on a half-open connection could stall the client for up to ~15 minutes; writes are now bounded by `pong_timeout`.

### Added

*   Typed `Error` enum (`Auth`, `Connection`, `NotConnected`, `ConnectionLost`, `Timeout`, `Stopped`, `Ha { code, message }`, …).
*   `ClientConfig` with connect/request timeouts, ping interval, pong timeout and reconnect backoff bounds.
*   `ConnectionState`, `connection_state()`, `watch_connection_state()`, `closed()`.
*   Keep-alive `ping`/`pong`; dead connections are detected and replaced.
*   `Event::Status { subscription, status }` with `SubscriptionStatus::{Interrupted, Restored, Rejected, Closed}`, delivered to each subscription's callback in order with its data, so handlers know when their data may be stale and can decide to wait or unsubscribe.
*   Bounded per-subscription event queues (`ClientConfig::subscription_queue_size`, default 1024). A callback that falls behind no longer grows memory without limit: entity subscriptions merge consecutive state changes, other subscriptions drop their oldest events, and the callback receives `SubscriptionStatus::Lagged { dropped }`. Status events are never dropped.
*   A warning is logged when a callback runs longer than `ClientConfig::slow_callback_warning` (default 5s).
*   `subscribe_entity` re-reads the entity after a reconnect: a change missed while disconnected is delivered as one `StateChanged` (last known → current) before `Restored`.
*   Exponential reconnect backoff with jitter (was a fixed 1 second). A token rejected during a reconnect stops the client (`ConnectionState::AuthFailed`) instead of retrying forever.
*   `subscribe_events`, `subscribe_trigger`, `subscribe_trigger_raw`, `call_service_with_response`.
*   `Other(String)` fallback on `Domain`, `Service`, `FanMode`, `SwingMode`, `PresetMode`, `DeviceClass`, `StateClass` and `UnitOfMeasurement`; more known domains and services.
*   `State::ha_state()`, `State::domain()`, `State::last_reported`, `AttributeSet::friendly_name()`, `HistoricalState::last_changed`, `Domain::of_entity()`.
*   `rustls-tls` feature.

### Changed

*   `subscribe_entity` uses a server-side `state` trigger, so only that entity's changes are transferred (previously each call received every `state_changed` event in the system).
*   Callbacks of one subscription run sequentially, in event order. Panics in callbacks are caught and logged.
*   `subscribe_*` methods return only after Home Assistant has accepted the subscription. `subscribe_entity` additionally fails with `not_found` if the entity does not exist (catches typos).
*   Subscribing while the client is reconnecting succeeds: the subscription starts as `Interrupted` and is sent when the connection is back. Plain requests still fail fast with `NotConnected`.
*   `State::ha_state()` for `climate` entities returns `HaState::Climate(HvacMode::Off)` instead of `HaState::Off`.
*   `call_service` passes the entity as `target` and accepts strings as well as enums.
*   `get_states` skips (and logs) entities it cannot parse instead of failing completely.
*   Internal modules (`connection`, `runner`, `protocol`, handlers) are no longer public.
*   `tracing-subscriber` is now a dev-dependency; the library logs through `log` only.

### Migration from 0.1

| 0.1 | 0.2 |
|---|---|
| `let mut c = HAClient::new(); c.run(url, token).await?;` | `let c = HAClient::connect(url, token).await?;` |
| `c.stop().await?` | `c.stop().await` |
| `anyhow::Result` from client methods | `ha_core_client::Result` (`Error` converts into `anyhow::Error` with `?`) |
| `Event::StateChanged { new_state: State, .. }` | `new_state: Option<State>` (`None` when the entity was removed) |
| `TriggerFiredEvent { name, description }` | `TriggerFiredEvent { trigger, context }` with `platform()`, `description()`, `entity_id()`, `from_state()`, `to_state()` |
| `get_history(id, start, end, Some(true))` | `get_history(id, start, end, true)` |
| `get_state_at_time(id, ts, chrono::Duration)` | `get_state_at_time(id, ts, chrono::TimeDelta)` (same type, new name) |
| `subscribe_sun_trigger(event, Option<std::time::Duration>, cb)` | `Option<chrono::TimeDelta>`, may be negative |
| `PresetMode::Custom(s)` | `PresetMode::Other(s)` |
| `UnitOfMeasurement::Custom(s)` | `UnitOfMeasurement::Other(s)` |
| `match` on `Domain`/`Service`/`DeviceClass`/… | add an `Other(_)` (or `_`) arm; the enums are `#[non_exhaustive]` |
| `Subscription { id: u32 }` | `Subscription::id() -> u64` |
| — | `match` on `Event` needs a wildcard arm: new `Event::Status` variant |
| `ha_core_client::subscription::HandlerCommand`, `runner`, `handler`, … | removed from the public API |

Callbacks keep the same shape: `Fn(HAClient, Event) -> impl Future<Output = anyhow::Result<()>>`.
