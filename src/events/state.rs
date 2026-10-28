use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use crate::events::attributes::{AttributeSet, ClimateAttributes, GenericAttributes, HistoricalAttributeSet};
use crate::events::helpers::de_helpers;
use crate::state::{Domain, HaState};

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
    #[serde(default, deserialize_with = "de_helpers::deserialize_optional_rfc3339_string")]
    pub last_reported: Option<DateTime<Utc>>,
    /// `{"id": ..., "parent_id": ..., "user_id": ...}`
    #[serde(default)]
    pub context: Value,
}

impl State {
    /// The state parsed into a typed [`HaState`].
    pub fn ha_state(&self) -> HaState {
        HaState::from_string(&self.state)
    }

    /// The entity's domain (`light`, `sensor`, …).
    pub fn domain(&self) -> Option<Domain> {
        Domain::of_entity(&self.entity_id)
    }

    /// Builds a `State` from the compressed format used by `subscribe_entities`
    /// (`{"s": ..., "a": ..., "c": ..., "lc": ..., "lu": ...}`).
    pub(crate) fn from_compressed(entity_id: &str, value: &Value) -> serde_json::Result<State> {
        let wire = CompressedStateWire::deserialize(value)?;
        let context = match wire.context {
            // Home Assistant sends only the id when there is no parent/user.
            Value::String(id) => json!({ "id": id, "parent_id": null, "user_id": null }),
            other => other,
        };
        Ok(State {
            entity_id: entity_id.to_string(),
            state: wire.state,
            attributes: wire.attributes,
            last_changed: wire.last_changed,
            last_updated: wire.last_updated.unwrap_or(wire.last_changed),
            last_reported: None,
            context,
        })
    }
}

#[derive(Deserialize)]
struct CompressedStateWire {
    #[serde(rename = "s")]
    state: String,
    #[serde(rename = "a", default)]
    attributes: AttributeSet,
    #[serde(rename = "c", default)]
    context: Value,
    #[serde(rename = "lc", deserialize_with = "de_helpers::deserialize_f64_unix_timestamp")]
    last_changed: DateTime<Utc>,
    #[serde(rename = "lu", default, deserialize_with = "de_helpers::deserialize_optional_f64_unix_timestamp")]
    last_updated: Option<DateTime<Utc>>,
}

/// Represents a single state entry from the Home Assistant history API.
/// It contains fewer fields than a live `State` object.
#[derive(Debug, Clone, Serialize)]
pub struct HistoricalState {
    pub entity_id: String,
    pub state: String,
    pub attributes: HistoricalAttributeSet,
    pub last_changed: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
}

/// A temporary struct to deserialize a single item from the history API response.
#[derive(Deserialize, Debug, Clone)]
pub(crate) struct HistoricalStateWire {
    #[serde(rename = "s")]
    pub state: String,
    #[serde(rename = "a", default)]
    pub attributes: HistoricalAttributeSet,
    #[serde(rename = "lu")]
    #[serde(deserialize_with = "de_helpers::deserialize_f64_unix_timestamp")]
    pub last_updated: DateTime<Utc>,
    /// Omitted by Home Assistant when equal to `lu`.
    #[serde(rename = "lc", default, deserialize_with = "de_helpers::deserialize_optional_f64_unix_timestamp")]
    pub last_changed: Option<DateTime<Utc>>,
}

impl HistoricalStateWire {
    pub(crate) fn into_state(self, entity_id: &str) -> HistoricalState {
        HistoricalState {
            entity_id: entity_id.to_string(),
            state: self.state,
            attributes: self.attributes,
            last_changed: self.last_changed.unwrap_or(self.last_updated),
            last_updated: self.last_updated,
        }
    }
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
            last_changed: historical.last_changed,
            last_updated: historical.last_updated,
            last_reported: None,
            context: json!({}),
        }
    }
}

/// The `data` field of a `state_changed` event (also the shape of a state trigger's
/// `variables.trigger`). `new_state` is `None` when the entity was removed,
/// `old_state` is `None` when it was added.
#[derive(Deserialize, Debug, Clone)]
pub(crate) struct StateChangedData {
    pub entity_id: String,
    #[serde(alias = "to_state")]
    pub new_state: Option<State>,
    #[serde(alias = "from_state")]
    pub old_state: Option<State>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Shape captured from a live `subscribe_entities` event (names anonymised).
    #[test]
    fn compressed_state_with_string_and_object_context() {
        let plain = json!({"s": "on", "a": {"friendly_name": "Lamp"}, "c": "01ABC", "lc": 1792706118.1049175});
        let s = State::from_compressed("light.lamp", &plain).unwrap();
        assert_eq!(s.context["id"], "01ABC");
        assert_eq!(s.last_updated, s.last_changed);
        assert_eq!(s.last_changed.timestamp_micros(), 1792706118104918);

        let with_user = json!({
            "s": "off", "a": {},
            "c": {"id": "01DEF", "parent_id": null, "user_id": "u1"},
            "lc": 1792706118.0, "lu": 1792706119.5
        });
        let s = State::from_compressed("switch.pump", &with_user).unwrap();
        assert_eq!(s.context["user_id"], "u1");
        assert_eq!(s.last_updated.timestamp_millis(), 1792706119500);
    }

    #[test]
    fn history_entry_without_lc_uses_lu() {
        let w: HistoricalStateWire =
            serde_json::from_value(json!({"s": "unavailable", "a": {"friendly_name": "T"}, "lu": 1793187541.0})).unwrap();
        let h = w.into_state("climate.t");
        assert_eq!(h.last_changed, h.last_updated);
    }

    #[test]
    fn state_changed_allows_missing_states() {
        let d: StateChangedData =
            serde_json::from_value(json!({"entity_id": "sensor.x", "old_state": null, "new_state": null})).unwrap();
        assert!(d.new_state.is_none() && d.old_state.is_none());
    }
}
