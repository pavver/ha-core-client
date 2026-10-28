# Changelog

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
*   The access token appeared in trace logs.

### Added

*   Typed `Error` enum (`Auth`, `Connection`, `NotConnected`, `ConnectionLost`, `Timeout`, `Stopped`, `Ha { code, message }`, …).
*   `ClientConfig` with connect/request timeouts, ping interval, pong timeout and reconnect backoff bounds.
*   `ConnectionState`, `connection_state()`, `watch_connection_state()`, `closed()`.
*   Keep-alive `ping`/`pong`; dead connections are detected and replaced.
*   Exponential reconnect backoff with jitter (was a fixed 1 second). A token rejected during a reconnect stops the client (`ConnectionState::AuthFailed`) instead of retrying forever.
*   `subscribe_events`, `subscribe_trigger`, `subscribe_trigger_raw`, `call_service_with_response`.
*   `Other(String)` fallback on `Domain`, `Service`, `FanMode`, `SwingMode`, `PresetMode`, `DeviceClass`, `StateClass` and `UnitOfMeasurement`; more known domains and services.
*   `State::ha_state()`, `State::domain()`, `State::last_reported`, `AttributeSet::friendly_name()`, `HistoricalState::last_changed`, `Domain::of_entity()`.
*   `rustls-tls` feature.

### Changed

*   `subscribe_entity` uses a server-side `state` trigger, so only that entity's changes are transferred (previously each call received every `state_changed` event in the system).
*   Callbacks of one subscription run sequentially, in event order. Panics in callbacks are caught and logged.
*   `subscribe_*` methods return only after Home Assistant has accepted the subscription.
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
| `ha_core_client::subscription::HandlerCommand`, `runner`, `handler`, … | removed from the public API |

Callbacks keep the same shape: `Fn(HAClient, Event) -> impl Future<Output = anyhow::Result<()>>`.
