# Home Assistant Core Client (ha-core-client)

A strongly-typed, ergonomic, and resilient asynchronous Rust client for the Home Assistant WebSocket API.

## Key Features

*   **Resilient connection**: reconnects with exponential backoff after network failures or Home Assistant restarts, restores all subscriptions, and detects dead connections with `ping`/`pong`.
*   **Honest failures**: a wrong URL or token is reported by `connect`; requests time out instead of hanging; rejected subscriptions return Home Assistant's error message. All errors are a typed `ha_core_client::Error`.
*   **Strongly typed, but tolerant**: enums for domains, services, states and attributes, each with an `Other(String)` fallback so values added by integrations or newer Home Assistant versions never break parsing.
*   **Server-side filtering**: `subscribe_entity` receives only that entity's changes, not every state change in the house.
*   **Ordered callbacks**: callbacks of one subscription run one after another, in event order; each receives an `HAClient` handle to make further calls.
*   **Triggers**: State, Numeric State, Sun (with negative offsets), Time, Time Pattern, Template, Zone, plus raw JSON for any other platform.
*   **History**: entity history and the state at a past point in time.

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

## Configuration

`HAClient::connect_with(ClientConfig)` tunes timeouts:

```rust
use std::time::Duration;
use ha_core_client::{ClientConfig, HAClient};

let config = ClientConfig::new("wss://ha.example.com/api/websocket", "TOKEN")
    .request_timeout(Duration::from_secs(10))   // default 30s
    .ping_interval(Duration::from_secs(30))     // default 30s
    .pong_timeout(Duration::from_secs(10))      // default 10s
    .reconnect_delay(Duration::from_secs(1), Duration::from_secs(60)); // backoff bounds
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

While reconnecting, requests fail fast with `Error::NotConnected`. If Home Assistant rejects the token during a reconnect, the client stops with `ConnectionState::AuthFailed` instead of retrying (repeated failed logins can get the client's IP banned).

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
*   `subscribe_events(event_type, callback)`, `subscribe_service_calls(callback)`: bus events.
*   `subscribe_trigger(Trigger, callback)`, `subscribe_trigger_raw(json, callback)`: any trigger, as `Event::TriggerFired`.
*   Helpers: `subscribe_state_trigger`, `subscribe_numeric_state_trigger`, `subscribe_sun_trigger`, `subscribe_time_trigger`, `subscribe_time_pattern_trigger`, `subscribe_template_trigger`, `subscribe_zone_trigger`.
*   `unsubscribe(subscription)`.

### Domain Helpers

*   **General**: `turn_on`, `turn_off`, `toggle` (any domain).
*   **Climate**: `set_hvac_mode`, `set_temperature`, `set_fan_mode`, `set_preset_mode`, `set_swing_mode`.
*   **Alarm**: `alarm_disarm`, `alarm_arm_home`, `alarm_arm_away`, `alarm_arm_night`, `alarm_arm_vacation`.
*   **Cover**: `open_cover`, `close_cover`, `stop_cover`, `set_cover_position`, `set_cover_tilt_position`.
*   **Lock**: `lock`, `unlock`.
*   **Input**: `set_number`, `set_text`, `select_option`.

## Development

```bash
cargo test                       # unit tests + end-to-end tests against a mock Home Assistant
cargo run --example demo         # read-only tour against a real instance
```

The demo reads `HA_URL`, `HA_TOKEN` and optionally `DEMO_ENTITY` from `config.json` (copy `config.example.json`; `config.json` is git-ignored).

See [CHANGELOG.md](CHANGELOG.md) for the migration guide from 0.1.

## License

GPL-3.0-only, see [LICENSE.md](LICENSE.md).
