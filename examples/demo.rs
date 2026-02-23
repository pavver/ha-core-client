use chrono::{DateTime, Duration, Local, NaiveDateTime, Utc, TimeZone}; 
use ha_core_client::state::climate::{FanMode, HvacMode, PresetMode, SwingMode};
use ha_core_client::HAClient;
use ha_core_client::triggers::SunEvent;
use log::{error, info, warn};
use tracing_subscriber::EnvFilter;
use anyhow::Result;
use std::time::Duration as StdDuration;

use std::fs::File;
use std::io::Write;
use std::ops::Add;
use std::path::Path;
use serde_json;
use std::future::Future;

// mod demo_config; // Removed

async fn handle_state_changed_event(
    _client: HAClient,
    event: ha_core_client::events::Event,
) -> Result<()> {
    info!("[Subscription Callback] Received state_changed event: {:?}", event);
    Ok(())
}

async fn handle_service_called_event(
    _client: HAClient,
    event: ha_core_client::events::Event,
) -> Result<()> {
    info!("[Subscription Callback] Received call_service event: {:?}", event);
    Ok(())
}

async fn handle_trigger_fired_event(
    _client: HAClient,
    event: ha_core_client::events::Event,
) -> Result<()> {
    info!("[Subscription Callback] Received trigger_fired event: {:?}", event);
    Ok(())
}


fn save_to_file(path: &str, text:&str)
{
    let _file = File::create(path);

    if _file.is_err() { return; }

    let mut file = _file.unwrap();

    // Write the text to the file
    let _ = file.write_all(text.as_bytes());
}

fn save_to_entities(file_name: &str, text:&str)
{
    // The file path where we want to save the text
    let mut path = Path::new("entities").join(file_name);
    path.set_extension("json");

    save_to_file(path.to_str().unwrap(), text);
}

async fn get_state(client: &HAClient, sensor_id:&str) {
    // get_state
    info!("Calling get_state for '{}'...", sensor_id);
    match client.get_state(sensor_id).await {
        Ok(state) => {
            info!(" -> Success: state is {:?}", state);
            match serde_json::to_string_pretty(&state) {
                Ok(json_string) => {
                    save_to_entities(sensor_id.to_string().add("_state").as_str(), &json_string);
                },
                Err(e) => {
                    error!("Failed to serialize state to JSON: {}", e);
                }
            }
        },
        Err(e) => error!(" -> Failed: {}", e),
    }
}

/// This function demonstrates the usage of all available client functions.
/// It's designed to be called once to ensure that all API helpers are wired correctly.
async fn run_demonstration(client: &HAClient) -> Result<()> {
    info!("--- Starting API Demonstration ---");

    // --- Placeholder Entity IDs ---
    let sensor_id = "sensor.battery_1_voltage";
    let binary_sensor = "binary_sensor.kulikivska_teritorialna_gromada_air";
    let alarm_id = "alarm_control_panel.test_alarm";
    let climate_id = "climate.esphome_gradka1_termostat_obigrivacha";
    let cover_id = "cover.test_blinds";
    let switch_id = "switch.ballast";
    let input_number_id = "input_number.maintain_humidity";
    let input_text_id = "input_text.test";
    let input_select_id = "input_select.test";
    let lock_id = "lock.test_lock";


    get_state(client, sensor_id).await;
    get_state(client, binary_sensor).await;
    get_state(client, input_number_id).await;
    get_state(client, input_select_id).await;
    get_state(client, climate_id).await;

    info!("1. Core Methods");

    // get_state_at_time
    info!("Calling get_state_at_time for '{}'...", sensor_id);
    let date_str = "2026-01-04 18:50:00";

    let format = "%Y-%m-%d %H:%M:%S";
    let naive_dt = NaiveDateTime::parse_from_str(date_str, format)
        .expect("Некоректний формат дати");

    let local_dt = Local.from_local_datetime(&naive_dt)
        .unwrap(); 

    let target_time: DateTime<Utc> = DateTime::from(local_dt);

    match client.get_state_at_time(sensor_id, target_time, Duration::minutes(5)).await {
        Ok(state) => info!(" -> Success: state at {} was {:?}", target_time, state),
        Err(e) => error!(" -> Failed: {}", e),
    }

    // subscribe_entity
    info!("Calling subscribe_entity for '{}'...", sensor_id);
    let entity_subscription = match client.subscribe_entity(sensor_id, handle_state_changed_event).await {
        Ok(sub) => {
            info!(" -> Success: subscribed to entity state changes.");
            Some(sub)
        },
        Err(e) => {
            error!(" -> Failed: {}", e);
            None
        }
    };

    // subscribe_service_calls
    info!("Calling subscribe_service_calls...");
    let service_subscription = match client.subscribe_service_calls(handle_service_called_event).await {
        Ok(sub) => {
            info!(" -> Success: subscribed to service calls.");
            Some(sub)
        },
        Err(e) => {
            error!(" -> Failed: {}", e);
            None
        }
    };

    // subscribe_state_trigger
    info!("Calling subscribe_state_trigger for '{}' to 'on'...", switch_id);
    let trigger_state_subscription = match client.subscribe_state_trigger(switch_id, Some(ha_core_client::state::HaState::On), None, Some(StdDuration::from_secs(5)), handle_trigger_fired_event).await {
        Ok(sub) => {
            info!(" -> Success: subscribed to state trigger.");
            Some(sub)
        },
        Err(e) => {
            error!(" -> Failed: {}", e);
            None
        }
    };

    // subscribe_numeric_state_trigger
    info!("Calling subscribe_numeric_state_trigger for '{}' above 10...", sensor_id);
    let trigger_numeric_state_subscription = match client.subscribe_numeric_state_trigger(sensor_id, Some(10.0), None, None, handle_trigger_fired_event).await {
        Ok(sub) => {
            info!(" -> Success: subscribed to numeric state trigger.");
            Some(sub)
        },
        Err(e) => {
            error!(" -> Failed: {}", e);
            None
        }
    };

    // subscribe_sun_trigger (sunrise event)
    info!("Calling subscribe_sun_trigger for sunrise...");
    let trigger_sun_subscription = match client.subscribe_sun_trigger(SunEvent::Sunrise, Some(StdDuration::from_secs(300)), handle_trigger_fired_event).await {
        Ok(sub) => {
            info!(" -> Success: subscribed to sun trigger (sunrise).");
            Some(sub)
        },
        Err(e) => {
            error!(" -> Failed: {}", e);
            None
        }
    };

    // subscribe_time_trigger (every minute at :00 seconds)
    info!("Calling subscribe_time_trigger for every minute at :00s...");
    let trigger_time_subscription = match client.subscribe_time_trigger("00:00:00".to_string(), handle_trigger_fired_event).await {
        Ok(sub) => {
            info!(" -> Success: subscribed to time trigger.");
            Some(sub)
        },
        Err(e) => {
            error!(" -> Failed: {}", e);
            None
        }
    };

    // subscribe_time_pattern_trigger (every 10 seconds)
    info!("Calling subscribe_time_pattern_trigger for every 10 seconds...");
    let trigger_time_pattern_subscription = match client.subscribe_time_pattern_trigger(None, None, Some("*/10".to_string()), handle_trigger_fired_event).await {
        Ok(sub) => {
            info!(" -> Success: subscribed to time pattern trigger.");
            Some(sub)
        },
        Err(e) => {
            error!(" -> Failed: {}", e);
            None
        }
    };

    // subscribe_template_trigger (example template)
    info!("Calling subscribe_template_trigger for a template...");
    let trigger_template_subscription = match client.subscribe_template_trigger("{{ states('sensor.battery_1_voltage') | float > 10.0 }}".to_string(), None, handle_trigger_fired_event).await {
        Ok(sub) => {
            info!(" -> Success: subscribed to template trigger.");
            Some(sub)
        },
        Err(e) => {
            error!(" -> Failed: {}", e);
            None
        }
    };

    // subscribe_zone_trigger (entity entering 'zone.home')
    info!("Calling subscribe_zone_trigger for entity entering 'zone.home'...");
    let trigger_zone_subscription = match client.subscribe_zone_trigger("person.test_person", "zone.home", ha_core_client::triggers::ZoneEvent::Enter, handle_trigger_fired_event).await {
        Ok(sub) => {
            info!(" -> Success: subscribed to zone trigger.");
            Some(sub)
        },
        Err(e) => {
            error!(" -> Failed: {}", e);
            None
        }
    };


    // --- Give subscriptions a moment to fire before running other commands ---
    info!("Waiting 2 seconds for any initial events...");
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // --- HomeAssistant Helpers (this will trigger the service_called event) ---
    info!("\n2. HomeAssistant Helpers");
    call_and_log("turn_on", client.turn_on(switch_id)).await;
    call_and_log("toggle", client.toggle(switch_id)).await;
    call_and_log("turn_off", client.turn_off(switch_id)).await;

    // --- Climate Helpers ---
    info!("\n4. Climate Helpers");
    call_and_log("set_hvac_mode", client.set_hvac_mode(climate_id, HvacMode::Heat)).await;
    call_and_log("set_temperature", client.set_temperature(climate_id, 22.5)).await;
    call_and_log("set_fan_mode", client.set_fan_mode(climate_id, FanMode::High)).await;
    call_and_log("set_preset_mode", client.set_preset_mode(climate_id, PresetMode::Eco)).await;
    call_and_log("set_swing_mode", client.set_swing_mode(climate_id, SwingMode::Both)).await;


    // --- Cover Helpers ---
    info!("\n5. Cover Helpers");
    call_and_log("open_cover", client.open_cover(cover_id)).await;
    call_and_log("close_cover", client.close_cover(cover_id)).await;
    call_and_log("stop_cover", client.stop_cover(cover_id)).await;
    call_and_log("set_cover_position", client.set_cover_position(cover_id, 50)).await;
    call_and_log("set_cover_tilt_position", client.set_cover_tilt_position(cover_id, 75)).await;

    // --- Input Helpers ---
    info!("\n6. Input Helpers");
    call_and_log("set_number", client.set_number(input_number_id, 42.0)).await;
    call_and_log("set_text", client.set_text(input_text_id, "Hello from client")).await;
    call_and_log("select_option", client.select_option(input_select_id, "Option A")).await;

    // --- Alarm Helpers ---
    info!("\n3. Alarm Helpers");
    call_and_log("alarm_disarm", client.alarm_disarm(alarm_id, Some("1234"))).await;
    call_and_log("alarm_arm_home", client.alarm_arm_home(alarm_id, Some("1234"))).await;
    call_and_log("alarm_arm_away", client.alarm_arm_away(alarm_id, Some("1234"))).await;
    call_and_log("alarm_arm_night", client.alarm_arm_night(alarm_id, Some("1234"))).await;
    call_and_log("alarm_arm_vacation", client.alarm_arm_vacation(alarm_id, Some("1234"))).await;

    // --- Lock Helpers ---
    info!("\n7. Lock Helpers");
    call_and_log("lock", client.lock(lock_id, None)).await;
    call_and_log("unlock", client.unlock(lock_id, None)).await;

    // --- Unsubscribe ---
    info!("\n8. Unsubscribing from events...");
    if let Some(sub) = entity_subscription {
        call_and_log("unsubscribe (entity)", client.unsubscribe(sub)).await;
    }
    if let Some(sub) = service_subscription {
        call_and_log("unsubscribe (service)", client.unsubscribe(sub)).await;
    }
    if let Some(sub) = trigger_state_subscription {
        call_and_log("unsubscribe (trigger_state)", client.unsubscribe(sub)).await;
    }
    if let Some(sub) = trigger_numeric_state_subscription {
        call_and_log("unsubscribe (trigger_numeric_state)", client.unsubscribe(sub)).await;
    }
    if let Some(sub) = trigger_sun_subscription {
        call_and_log("unsubscribe (trigger_sun)", client.unsubscribe(sub)).await;
    }
    if let Some(sub) = trigger_time_subscription {
        call_and_log("unsubscribe (trigger_time)", client.unsubscribe(sub)).await;
    }
    if let Some(sub) = trigger_time_pattern_subscription {
        call_and_log("unsubscribe (trigger_time_pattern)", client.unsubscribe(sub)).await;
    }
    if let Some(sub) = trigger_template_subscription {
        call_and_log("unsubscribe (trigger_template)", client.unsubscribe(sub)).await;
    }
    if let Some(sub) = trigger_zone_subscription {
        call_and_log("unsubscribe (trigger_zone)", client.unsubscribe(sub)).await;
    }

    info!("\n--- API Demonstration Finished ---");
    Ok(())
}

/// Helper to call an async function and log its result.
async fn call_and_log(name: &str, fut: impl Future<Output = Result<()>>) {
    info!("Calling {}...", name);
    match fut.await {
        Ok(_) => info!(" -> Success"),
        // Don't treat this as a fatal error, just a warning, as the entity likely doesn't exist.
        Err(e) => warn!(" -> Failed (as expected for non-existent entity): {}", e),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // --- Setup ---
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let cfg = read_config();
    let mut client = HAClient::new();

    // --- Main Logic ---
    client.run(&cfg.url, &cfg.token).await?;
    info!("Client is connected and running.");

    // Run the demonstration of all functions
    if let Err(e) = run_demonstration(&client).await {
        error!("Demonstration failed: {}", e);
    }
    
    // Give subscription a moment to fire
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    // Stop the client
    info!("Shutting down client...");
    client.stop().await?;
    info!("Client has been stopped.");

    Ok(())
}
use serde_json::Value;
use std::fs;

pub struct Config {
    pub url: String,
    pub token: String,
}

fn config_read_key<'a>(config: &'a Value, key: &'a str) -> &'a str {
    match &config[key] {
        Value::String(s) => return s.as_str(),
        _default => panic!("Failed to read '{}' in the config file", key),
    }
}

pub fn read_config() -> Config {
    let file_path = "config.json";
    let file_content = fs::read_to_string(file_path).expect("Failed to read the config file");
    let config: Value =
        serde_json::from_str(&file_content).expect("Failed to parse the config file");

    let url = config_read_key(&config, "HA_URL");
    let token = config_read_key(&config, "HA_TOKEN");

    Config {
        url: url.to_string(),
        token: token.to_string(),
    }
}
