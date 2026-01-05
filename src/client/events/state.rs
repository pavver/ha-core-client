use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use crate::client::events::attributes::{AttributeSet, ClimateAttributes, GenericAttributes, HistoricalAttributeSet};
use crate::client::events::helpers::de_helpers;

/// Represents the detailed state of a Home Assistant entity from a live event.
#[derive(Debug, Clone, Deserialize, Serialize)]
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

/// Represents a single state entry from the Home Assistant history API.
/// It contains fewer fields than a live `State` object.
#[derive(Debug, Clone, Serialize)]
pub struct HistoricalState {
    pub entity_id: String,
    pub state: String,
    pub attributes: HistoricalAttributeSet,
    pub last_updated: DateTime<Utc>,
}

/// A temporary struct to deserialize a single item from the history API response.
#[derive(Deserialize, Debug, Clone, Serialize)]
pub(crate) struct HistoricalStateWire {
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
#[derive(Deserialize, Debug, Clone, Serialize)]
pub(crate) struct StateChangedData {
    pub entity_id: String,
    pub new_state: State,
    pub old_state: Option<State>,
}
