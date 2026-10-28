# Home Assistant Core Client (ha-core-client)

A strongly-typed, ergonomic, and resilient asynchronous Rust client for the Home Assistant WebSocket API.

## Key Features

*   **Resilient connection**: reconnects with exponential backoff after network failures or Home Assistant restarts, restores all subscriptions, and detects dead connections with `ping`/`pong`.
*   **Honest failures**: a wrong URL or token is reported by `connect`; requests time out instead of hanging; rejected subscriptions return Home Assistant's error message. All errors are a typed `ha_core_client::Error`.
*   **Strongly typed, but tolerant**: enums for domains, services, states and attributes, each with an `Other(String)` fallback so values added by integrations or newer Home Assistant versions never break parsing.
*   **Server-side filtering**: `subscribe_entity` receives only that entity's changes, not every state change in the house.
*   **Ordered callbacks**: callbacks of one subscription run one after another, in event order; each receives an `HAClient` handle to make further calls.
*   **Triggers**: State, Numeric State, Sun (with negative offsets), Time, Time Pattern, Template, Zone (with builders), plus raw JSON for any other platform.
*   **Templates**: subscribe to a template rendered by Home Assistant, and detect sensors that stopped reporting (`watch_silence`) without polling.
*   **History**: entity history and the state at a past point in time.

## Supported Home Assistant versions

Home Assistant **2024.10 or newer**: triggers are sent with the `trigger` key (older versions only accept `platform`).

## Quick Start

```toml
[dependencies]
ha-core-client = { git = "https://github.com/pavver/ha-core-client" }
tokio = { version = "1", features = ["full"] }
anyhow = "1"
```

```rust
use ha_core_client::{HAClient, events::Event};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Fails right away on a wrong URL or token; later disconnects are retried.
    let client = HAClient::connect("ws://192.168.1.100:8123/api/websocket", "YOUR_TOKEN").await?;

    client.subscribe_entity("binary_sensor.motion", |client, event| async move {
        if let Event::StateChanged { new_state: Some(state), .. } = event {
            if state.state == "on" {
                client.turn_on("light.hallway").await?;
            }
        }
        Ok(())
    }).await?;

    let temperature = client.get_state("sensor.temperature").await?;
    println!("Current temperature: {}", temperature.state);

    tokio::signal::ctrl_c().await?;
    client.stop().await;
    Ok(())
}
```

Keep at least one `HAClient` handle alive: when every handle is dropped, the client shuts down.

Callbacks return `ha_core_client::CallbackResult` (`Result<(), Box<dyn Error + Send + Sync>>`): end with `Ok(())`, and `?` works on any error type, including `anyhow::Error`. Returned errors are logged.

## Configuration

`HAClient::connect_with(ClientConfig)` tunes timeouts:

```rust
use std::time::Duration;
use ha_core_client::{ClientConfig, HAClient};

let config = ClientConfig::new("wss://ha.example.com/api/websocket", "TOKEN")
    .connect_timeout(Duration::from_secs(10))   // default 10s
    .request_timeout(Duration::from_secs(10))   // default 30s
    .ping_interval(Duration::from_secs(30))     // default 30s
    .pong_timeout(Duration::from_secs(10))      // default 10s
    .reconnect_delay(Duration::from_secs(1), Duration::from_secs(60)) // backoff bounds
    .subscription_queue_size(1024)              // events waiting per subscription
    .slow_callback_warning(Duration::from_secs(5)) // log callbacks that run this long
    .max_message_size(64 << 20);                // largest accepted message, default 64 MiB
let client = HAClient::connect_with(config).await?;
```

TLS for `wss://` uses `native-tls` by default; enable the `rustls-tls` feature (with `default-features = false`) to use rustls instead.

## Connection State

```rust
use ha_core_client::ConnectionState;

let mut states = client.watch_connection_state();
while states.changed().await.is_ok() {
    match &*states.borrow() {
        ConnectionState::Connected => println!("connected"),
        ConnectionState::Reconnecting { attempt } => println!("reconnecting (attempt {attempt})"),
        ConnectionState::AuthFailed(reason) => println!("token rejected: {reason}"),
        _ => break,
    }
}
```

Every subscription callback is also told about the connection, in order with its data:

```rust
use ha_core_client::{events::Event, SubscriptionStatus};

client.subscribe_entity("sensor.power", |client, event| async move {
    match event {
        Event::StateChanged { new_state, .. } => { /* use the new state */ }
        Event::Status { status: SubscriptionStatus::Interrupted, .. } => { /* data may be stale now */ }
        Event::Status { status: SubscriptionStatus::Restored, .. } => { /* live again */ }
        Event::Status { subscription, status: SubscriptionStatus::Rejected { message, .. } } => {
            eprintln!("subscription {} dropped by Home Assistant: {message}", subscription.id());
        }
        _ => {}
    }
    Ok(())
}).await?;
```

Callbacks of one subscription run one after another. If a callback falls behind, `subscribe_entity` merges the waiting state changes into one (from the last delivered state to the newest), and other subscriptions drop their oldest events beyond `subscription_queue_size`; the callback is told with `SubscriptionStatus::Lagged { dropped }`. To process events concurrently, `tokio::spawn` inside the callback.

After a reconnect, `subscribe_entity` re-reads the entity: if it changed while disconnected, one `StateChanged` (last known → current state) arrives right before `Restored`, and live changes follow only after it. A handler can also `client.unsubscribe(subscription)` on `Interrupted`, which works while offline.

`unsubscribe` ends with `SubscriptionStatus::Unsubscribed`; `stop()` waits (up to 5s) for callbacks to handle what is queued, ending with `Closed`.

While reconnecting, requests fail fast with `Error::NotConnected`, but new subscriptions are accepted: they start as `Interrupted` and are sent once the connection is back. After a reconnect, triggers are re-created in Home Assistant, so a pending `for` countdown starts over. If Home Assistant rejects the token during a reconnect, the client stops with `ConnectionState::AuthFailed` instead of retrying (repeated failed logins can get the client's IP banned).

## API Overview

### Core `HAClient` Methods

*   `connect(url, token)` / `connect_with(config)`: connects and authenticates.
*   `stop()`, `closed()`, `connection_state()`, `watch_connection_state()`.
*   `get_state(entity_id)`, `get_states()`.
*   `get_history(entity_id, start, end, significant_changes_only)`.
*   `get_state_at_time(entity_id, timestamp, lookback)`.
*   `call_service(domain, service, entity_id, data)`: domains and services can be enums or strings (`call_service("vacuum", "start", ...)`).
*   `call_service_with_response(...)`: for services that return data, such as `weather.get_forecasts`.

### Subscriptions

*   `subscribe_entity(entity_id, callback)`: state/attribute changes of one entity, as `Event::StateChanged`.
*   `subscribe_entity_with(entity_id, EntityChanges::State, callback)`: only changes of the state itself, skipping attribute-only updates. (A sensor re-reporting an unchanged value is never delivered: Home Assistant does not send such reports over the WebSocket API.)
*   `watch_silence("sensor.outdoor", Duration::from_secs(1800), callback)`: `Event::Silence { silent }` when a sensor has not reported (even an unchanged value) for longer than the given time, and when it reports again. Checked in Home Assistant, with no polling.
*   `subscribe_template("{{ states('sensor.x') | float * 2 }}", callback)`: `Event::TemplateRendered { result }` with the current result and on every change.
*   `subscribe_events(event_type, callback)`, `subscribe_service_calls(callback)`: bus events.
*   `subscribe_trigger(trigger, callback)`: a trigger built with `StateTrigger`, `NumericStateTrigger`, `SunTrigger`, `TimeTrigger`, `TimePatternTrigger`, `TemplateTrigger` or `ZoneTrigger` (`triggers` module), as `Event::TriggerFired`:
    ```rust
    client.subscribe_trigger(StateTrigger::new("binary_sensor.door").to(HaState::On).for_duration(Duration::from_secs(60)), callback).await?;
    client.subscribe_trigger(TimePatternTrigger::new().minutes("/5"), callback).await?;
    ```
*   `subscribe_trigger_raw(json!({"trigger": "mqtt", "topic": "x"}), callback)`: any other platform (see [TRIGGERS_OVERVIEW.md](TRIGGERS_OVERVIEW.md)).
*   `unsubscribe(subscription)`, or `subscription.guard(&client)` to unsubscribe on drop.

### Domain Helpers

*   **General**: `turn_on`, `turn_off`, `toggle` (any domain).
*   **Climate**: `set_hvac_mode`, `set_temperature`, `set_temperature_range`, `set_fan_mode`, `set_preset_mode`, `set_swing_mode`.
*   **Alarm**: `alarm_disarm`, `alarm_arm_home`, `alarm_arm_away`, `alarm_arm_night`, `alarm_arm_vacation`.
*   **Cover**: `open_cover`, `close_cover`, `stop_cover`, `set_cover_position`, `set_cover_tilt_position`.
*   **Lock**: `lock`, `unlock`.
*   **Input**: `set_number`, `set_text`, `select_option` (`input_*` or `number`/`text`/`select` entities).

## Development

```bash
cargo test                       # unit tests + end-to-end tests against a mock Home Assistant
cargo run --example demo         # read-only tour against a real instance
```

The demo reads `HA_URL`, `HA_TOKEN` and optionally `DEMO_ENTITY` from `config.json` (copy `config.example.json`; `config.json` is git-ignored).

See [CHANGELOG.md](CHANGELOG.md) for the migration guides from 0.1 and 0.2.

## License

GPL-3.0-only, see [LICENSE.md](LICENSE.md).
