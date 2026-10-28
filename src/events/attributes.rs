use std::collections::HashMap;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use crate::state::climate::{FanMode, HvacMode, PresetMode, SwingMode};
use crate::state::common::UnitOfMeasurement;
use crate::state::device_class::DeviceClass;
use crate::state::state_class::StateClass;

#[derive(Debug, Clone, Deserialize, Serialize)]
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

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct GenericAttributes {
    pub friendly_name: Option<String>,
    pub unit_of_measurement: Option<UnitOfMeasurement>,
    pub device_class: Option<DeviceClass>,
    pub state_class: Option<StateClass>,
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}

/// Entity attributes. `Climate` is chosen when climate-specific keys are present
/// and they parse; everything else (including malformed climate attributes) is `Generic`.
#[derive(Debug, Clone, Serialize)]
pub enum AttributeSet {
    Climate(ClimateAttributes),
    Generic(GenericAttributes),
}

impl AttributeSet {
    /// The entity's `friendly_name`, if set.
    pub fn friendly_name(&self) -> Option<&str> {
        match self {
            AttributeSet::Climate(a) => a.rest.get("friendly_name").and_then(Value::as_str),
            AttributeSet::Generic(a) => a.friendly_name.as_deref(),
        }
    }
}

/// Parses `GenericAttributes`, never failing: if a known key has an unexpected type,
/// the typed fields are left empty and every key is kept in `rest`.
fn lenient_generic<T: for<'de> Deserialize<'de> + Default>(value: Value, rest: impl FnOnce(&mut T, Map<String, Value>)) -> T {
    match T::deserialize(&value) {
        Ok(attrs) => attrs,
        Err(e) => {
            log::debug!("Attributes did not match the typed schema ({e}); keeping them untyped.");
            let mut attrs = T::default();
            if let Value::Object(map) = value {
                rest(&mut attrs, map);
            }
            attrs
        }
    }
}

impl<'de> Deserialize<'de> for AttributeSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;

        if let Value::Object(map) = &value {
            if map.contains_key("hvac_modes") || map.contains_key("current_temperature") {
                if let Ok(climate_attrs) = ClimateAttributes::deserialize(&value) {
                    return Ok(AttributeSet::Climate(climate_attrs));
                }
            }
        }

        Ok(AttributeSet::Generic(lenient_generic(value, |a: &mut GenericAttributes, m| a.rest = m)))
    }
}

impl Default for AttributeSet {
    fn default() -> Self {
        AttributeSet::Generic(GenericAttributes::default())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HistoricalClimateAttributes {
    pub friendly_name: Option<String>,
    pub current_temperature: Option<f64>,
    pub temperature: Option<f64>,
    #[serde(flatten)]
    pub rest: HashMap<String, Value>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct HistoricalGenericAttributes {
    pub friendly_name: Option<String>,
    pub unit_of_measurement: Option<UnitOfMeasurement>,
    pub device_class: Option<DeviceClass>,
    pub state_class: Option<StateClass>,
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
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
                if let Ok(climate_attrs) = HistoricalClimateAttributes::deserialize(&value) {
                    return Ok(HistoricalAttributeSet::Climate(climate_attrs));
                }
            }
        }

        Ok(HistoricalAttributeSet::Generic(lenient_generic(
            value,
            |a: &mut HistoricalGenericAttributes, m| a.rest = m,
        )))
    }
}

impl Default for HistoricalAttributeSet {
    fn default() -> Self {
        HistoricalAttributeSet::Generic(HistoricalGenericAttributes::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unknown_enum_values_do_not_fail() {
        let a: AttributeSet = serde_json::from_value(json!({
            "hvac_modes": ["off", "heat"],
            "fan_modes": ["quiet", "turbo"],
            "current_temperature": 21,
            "friendly_name": "Thermostat"
        }))
        .unwrap();
        let AttributeSet::Climate(c) = &a else { panic!("expected climate: {a:?}") };
        assert_eq!(c.fan_modes.as_ref().unwrap()[0], FanMode::Other("quiet".into()));
        assert_eq!(a.friendly_name(), Some("Thermostat"));

        let a: AttributeSet = serde_json::from_value(json!({"device_class": "wind_direction"})).unwrap();
        let AttributeSet::Generic(g) = a else { panic!() };
        assert_eq!(g.device_class, Some(DeviceClass::Other("wind_direction".into())));
    }

    #[test]
    fn malformed_attributes_fall_back_to_untyped() {
        // `current_temperature` as a string breaks the climate schema,
        // `friendly_name` as a number breaks the generic one.
        let a: AttributeSet =
            serde_json::from_value(json!({"current_temperature": "n/a", "friendly_name": 5})).unwrap();
        let AttributeSet::Generic(g) = a else { panic!() };
        assert_eq!(g.friendly_name, None);
        assert_eq!(g.rest["friendly_name"], 5);
        assert_eq!(g.rest["current_temperature"], "n/a");
    }
}
