use crate::client::state::climate::{FanMode, HvacMode, PresetMode, SwingMode};
use crate::client::state::common::UnitOfMeasurement;
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Deserializer};
use serde_json::{json, Map, Value};
use std::collections::HashMap;

// --- Deserialization Helpers ---

pub(crate) mod de_helpers {
    use super::*;

    /// Deserializes an RFC3339 string into a `DateTime<Utc>`.
    pub fn deserialize_rfc3339_string<'de, D>(
        deserializer: D,
    ) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        DateTime::parse_from_rfc3339(&s)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| {
                serde::de::Error::custom(format!("Invalid timestamp format: {}, error: {}", s, e))
            })
    }

    /// Deserializes a floating-point Unix timestamp into a `DateTime<Utc>`.
    pub fn deserialize_f64_unix_timestamp<'de, D>(
        deserializer: D,
    ) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ts_f64 = f64::deserialize(deserializer)?;
        let secs = ts_f64 as i64;
        let nanos = ((ts_f64 - secs as f64) * 1e9) as u32;
        Utc.timestamp_opt(secs, nanos)
            .single()
            .ok_or_else(|| serde::de::Error::custom("Invalid Unix timestamp value"))
    }
}

// --- Attribute Structures ---

#[derive(Debug, Clone, Deserialize)]
pub struct ClimateAttributes {
    pub hvac_modes: Option<Vec<HvacMode>>,
    pub min_temp: Option<f64>,
    pub max_temp: Option<f64>,
    pub current_temperature: Option<f64>,
    pub temperature: Option<f64>,
    pub fan_modes: Option<Vec<FanMode>>,
    pub swing_modes: Option<Vec<SwingMode>>,
    pub preset_modes: Option<Vec<PresetMode>>,
    #[serde(flatten)]
    pub rest: HashMap<String, Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GenericAttributes {
    pub friendly_name: Option<String>,
    pub unit_of_measurement: Option<UnitOfMeasurement>,
    pub device_class: Option<String>,
    pub state_class: Option<String>,
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}

#[derive(Debug, Clone)]
pub enum AttributeSet {
    Climate(ClimateAttributes),
    Generic(GenericAttributes),
}

impl<'de> Deserialize<'de> for AttributeSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;

        // Try to deserialize as ClimateAttributes first if climate-specific keys are present.
        if let Value::Object(map) = &value {
            let keys: Vec<_> = map.keys().map(String::as_str).collect();
            if keys.contains(&"hvac_modes") || keys.contains(&"current_temperature") {
                let climate_attrs = ClimateAttributes::deserialize(value)
                    .map_err(serde::de::Error::custom)?;
                return Ok(AttributeSet::Climate(climate_attrs));
            }
        }

        // Fallback to GenericAttributes
        let generic_attrs =
            GenericAttributes::deserialize(value).map_err(serde::de::Error::custom)?;
        Ok(AttributeSet::Generic(generic_attrs))
    }
}

// --- Event and State Structures ---

/// Represents the detailed state of a Home Assistant entity from a live event.
#[derive(Debug, Clone, Deserialize)]
pub struct State {
    pub entity_id: String,
    pub state: String,
    #[serde(default)]
    pub attributes: AttributeSet,
    #[serde(deserialize_with = "de_helpers::deserialize_rfc3339_string")]
    pub last_changed: DateTime<Utc>,
    #[serde(deserialize_with = "de_helpers::deserialize_rfc3339_string")]
    pub last_updated: DateTime<Utc>,
    pub context: Value,
}

impl Default for AttributeSet {
    fn default() -> Self {
        AttributeSet::Generic(GenericAttributes {
            friendly_name: None,
            unit_of_measurement: None,
            device_class: None,
            state_class: None,
            rest: Map::new(),
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct HistoricalClimateAttributes {
    pub friendly_name: Option<String>,
    pub current_temperature: Option<f64>,
    pub temperature: Option<f64>,
    #[serde(flatten)]
    pub rest: HashMap<String, Value>,
}
#[derive(Debug, Clone, Deserialize)]
pub struct HistoricalGenericAttributes {
    pub friendly_name: Option<String>,
    pub unit_of_measurement: Option<UnitOfMeasurement>,
    pub device_class: Option<String>,
    pub state_class: Option<String>,
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}
#[derive(Debug, Clone)]
pub enum HistoricalAttributeSet {
    Climate(HistoricalClimateAttributes),
    Generic(HistoricalGenericAttributes),
}

impl<'de> Deserialize<'de> for HistoricalAttributeSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;

        if let Value::Object(map) = &value {
            if map.contains_key("current_temperature") {
                let climate_attrs = HistoricalClimateAttributes::deserialize(value)
                    .map_err(serde::de::Error::custom)?;
                return Ok(HistoricalAttributeSet::Climate(climate_attrs));
            }
        }

        let generic_attrs = HistoricalGenericAttributes::deserialize(value)
            .map_err(serde::de::Error::custom)?;
        Ok(HistoricalAttributeSet::Generic(generic_attrs))
    }
}

impl Default for HistoricalAttributeSet {
    fn default() -> Self {
        HistoricalAttributeSet::Generic(HistoricalGenericAttributes {
            friendly_name: None,
            unit_of_measurement: None,
            device_class: None,
            state_class: None,
            rest: Map::new(),
        })
    }
}
/// Represents a single state entry from the Home Assistant history API.
/// It contains fewer fields than a live `State` object.
#[derive(Debug, Clone)]
pub struct HistoricalState {
    pub entity_id: String,
    pub state: String,
    pub attributes: HistoricalAttributeSet,
    pub last_updated: DateTime<Utc>,
}

/// A temporary struct to deserialize a single item from the history API response.
#[derive(Deserialize, Debug, Clone)]
pub(super) struct HistoricalStateWire {
    #[serde(rename = "s")]
    pub state: String,
    #[serde(rename = "a", default)]
    pub attributes: HistoricalAttributeSet,
    #[serde(rename = "lu")]
    #[serde(deserialize_with = "de_helpers::deserialize_f64_unix_timestamp")]
    pub last_updated: DateTime<Utc>,
}

impl From<HistoricalState> for State {
    fn from(historical: HistoricalState) -> Self {
        let attributes = match historical.attributes {
            HistoricalAttributeSet::Climate(hist_attrs) => {
                let mut rest_map = HashMap::new();
                if let Some(friendly_name) = hist_attrs.friendly_name {
                    rest_map.insert(
                        "friendly_name".to_string(),
                        Value::String(friendly_name),
                    );
                }
                rest_map.extend(hist_attrs.rest);

                AttributeSet::Climate(ClimateAttributes {
                    hvac_modes: None,
                    min_temp: None,
                    max_temp: None,
                    current_temperature: hist_attrs.current_temperature,
                    temperature: hist_attrs.temperature,
                    fan_modes: None,
                    swing_modes: None,
                    preset_modes: None,
                    rest: rest_map,
                })
            }
            HistoricalAttributeSet::Generic(hist_attrs) => {
                AttributeSet::Generic(GenericAttributes {
                    friendly_name: hist_attrs.friendly_name,
                    unit_of_measurement: hist_attrs.unit_of_measurement,
                    device_class: hist_attrs.device_class,
                    state_class: hist_attrs.state_class,
                    rest: hist_attrs.rest,
                })
            }
        };

        Self {
            entity_id: historical.entity_id,
            state: historical.state,
            attributes,
            last_changed: historical.last_updated,
            last_updated: historical.last_updated,
            context: json!({}),
        }
    }
}

/// A temporary struct to deserialize the `data` field of a `state_changed` event.
/// This is an internal implementation detail.
#[derive(Deserialize, Debug, Clone)]
pub(super) struct StateChangedData {
    pub entity_id: String,
    pub new_state: State,
    pub old_state: Option<State>,
}

/// Represents a deserialized, type-safe Home Assistant event.
/// This enum is passed to event callbacks.
#[derive(Debug, Clone)]
pub enum Event {
    /// Corresponds to the `state_changed` event type.
    /// Triggered when the state of a subscribed entity changes.
    StateChanged {
        entity_id: String,
        new_state: State,
        old_state: Option<State>,
    },

    /// A generic fallback for any event that doesn't have a specific
    /// type-safe variant in this enum. It provides the raw `event_type`
    /// string and the JSON `data` payload.
    Generic {
        event_type: String,
        data: Value,
    },
}
