use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use crate::events::attributes::AttributeSet;
use crate::events::helpers::de_helpers;
use crate::state::{Domain, HaState};

/// Represents the detailed state of a Home Assistant entity from a live event.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(from = "StateWire")]
pub struct State {
    pub entity_id: String,
    pub state: String,
    /// Typed by the entity's domain, see [`AttributeSet`].
    pub attributes: AttributeSet,
    pub last_changed: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
    pub last_reported: Option<DateTime<Utc>>,
    /// `{"id": ..., "parent_id": ..., "user_id": ...}`
    pub context: Value,
}

#[derive(Deserialize)]
struct StateWire {
    entity_id: String,
    state: String,
    #[serde(default)]
    attributes: Value,
    #[serde(deserialize_with = "de_helpers::deserialize_rfc3339_string")]
    last_changed: DateTime<Utc>,
    #[serde(deserialize_with = "de_helpers::deserialize_rfc3339_string")]
    last_updated: DateTime<Utc>,
    #[serde(default, deserialize_with = "de_helpers::deserialize_optional_rfc3339_string")]
    last_reported: Option<DateTime<Utc>>,
    #[serde(default)]
    context: Value,
}

impl From<StateWire> for State {
    fn from(w: StateWire) -> Self {
        State {
            attributes: AttributeSet::for_entity(&w.entity_id, w.attributes),
            entity_id: w.entity_id,
            state: w.state,
            last_changed: w.last_changed,
            last_updated: w.last_updated,
            last_reported: w.last_reported,
            context: w.context,
        }
    }
}

impl State {
    /// The state parsed into a typed [`HaState`], using the entity's domain to resolve
    /// ambiguous values (see [`HaState::parse_for_domain`]).
    pub fn ha_state(&self) -> HaState {
        match self.domain() {
            Some(domain) => HaState::parse_for_domain(&domain, &self.state),
            None => HaState::from_string(&self.state),
        }
    }

    /// The entity's domain (`light`, `sensor`, …).
    pub fn domain(&self) -> Option<Domain> {
        Domain::of_entity(&self.entity_id)
    }

    /// When the entity last reported, even an unchanged value: `last_reported`, falling
    /// back to `last_updated` where Home Assistant does not send it (`get_state`, history).
    pub fn last_seen(&self) -> DateTime<Utc> {
        self.last_reported.map_or(self.last_updated, |reported| reported.max(self.last_updated))
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
            attributes: AttributeSet::for_entity(entity_id, wire.attributes),
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
    attributes: Value,
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
    /// Typed by the entity's domain, see [`AttributeSet`].
    pub attributes: AttributeSet,
    pub last_changed: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
}

/// A temporary struct to deserialize a single item from the history API response.
#[derive(Deserialize, Debug, Clone)]
pub(crate) struct HistoricalStateWire {
    #[serde(rename = "s")]
    pub state: String,
    #[serde(rename = "a", default)]
    pub attributes: Value,
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
            attributes: AttributeSet::for_entity(entity_id, self.attributes),
            last_changed: self.last_changed.unwrap_or(self.last_updated),
            last_updated: self.last_updated,
        }
    }
}

impl From<HistoricalState> for State {
    fn from(historical: HistoricalState) -> Self {
        Self {
            entity_id: historical.entity_id,
            state: historical.state,
            attributes: historical.attributes,
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

    #[test]
    fn last_seen_prefers_last_reported() {
        let mut s = State::from_compressed("sensor.t", &json!({"s": "1", "a": {}, "c": "x", "lc": 100.0})).unwrap();
        assert_eq!(s.last_seen(), s.last_updated);
        let reported = s.last_updated + chrono::TimeDelta::seconds(30);
        s.last_reported = Some(reported);
        assert_eq!(s.last_seen(), reported);
    }

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
    fn attributes_are_typed_by_domain() {
        let attrs = json!({"current_temperature": 48, "temperature": 55, "friendly_name": "Boiler"});
        let full = |entity_id: &str| -> State {
            serde_json::from_value(json!({
                "entity_id": entity_id, "state": "eco", "attributes": attrs,
                "last_changed": "2026-10-28T14:45:09.185007+00:00",
                "last_updated": "2026-10-28T14:45:09.185007+00:00"
            }))
            .unwrap()
        };
        // A water heater has climate-like keys but is not a climate entity.
        assert!(matches!(full("water_heater.boiler").attributes, AttributeSet::Generic(_)));
        assert!(matches!(full("climate.boiler").attributes, AttributeSet::Climate(_)));

        let compressed = State::from_compressed("water_heater.boiler", &json!({"s": "eco", "a": attrs, "lc": 1.0})).unwrap();
        assert!(matches!(compressed.attributes, AttributeSet::Generic(_)));
        let w: HistoricalStateWire = serde_json::from_value(json!({"s": "heat", "a": attrs, "lu": 1.0})).unwrap();
        assert!(matches!(w.into_state("climate.t").attributes, AttributeSet::Climate(_)));
    }

    #[test]
    fn state_changed_allows_missing_states() {
        let d: StateChangedData =
            serde_json::from_value(json!({"entity_id": "sensor.x", "old_state": null, "new_state": null})).unwrap();
        assert!(d.new_state.is_none() && d.old_state.is_none());
    }
}
