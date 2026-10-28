use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use crate::state::Domain;
use crate::state::climate::{FanMode, HvacAction, HvacMode, PresetMode, SwingMode};
use crate::state::common::UnitOfMeasurement;
use crate::state::device_class::DeviceClass;
use crate::state::state_class::StateClass;

/// Attributes of a `climate` entity. The current HVAC mode is the entity's state.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[non_exhaustive]
pub struct ClimateAttributes {
    pub friendly_name: Option<String>,
    /// Modes the entity supports. Values this library does not know are left out.
    #[serde(default, deserialize_with = "known_values")]
    pub hvac_modes: Option<Vec<HvacMode>>,
    pub hvac_action: Option<HvacAction>,
    pub current_temperature: Option<f64>,
    pub current_humidity: Option<f64>,
    /// Target temperature; `None` in `heat_cool` mode, which uses the range below.
    pub temperature: Option<f64>,
    pub target_temp_low: Option<f64>,
    pub target_temp_high: Option<f64>,
    pub target_temp_step: Option<f64>,
    pub min_temp: Option<f64>,
    pub max_temp: Option<f64>,
    pub fan_mode: Option<FanMode>,
    pub fan_modes: Option<Vec<FanMode>>,
    pub preset_mode: Option<PresetMode>,
    pub preset_modes: Option<Vec<PresetMode>>,
    pub swing_mode: Option<SwingMode>,
    pub swing_modes: Option<Vec<SwingMode>>,
    /// Every other attribute (`supported_features`, integration-specific keys, …).
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}

/// Attributes of any entity without a dedicated type.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[non_exhaustive]
pub struct GenericAttributes {
    pub friendly_name: Option<String>,
    pub unit_of_measurement: Option<UnitOfMeasurement>,
    pub device_class: Option<DeviceClass>,
    pub state_class: Option<StateClass>,
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}

/// Entity attributes, typed by the entity's domain: `Climate` for `climate.*` entities
/// whose attributes parse, `Generic` for everything else (including malformed climate
/// attributes). Deserialized on its own, without an entity id, `Climate` is chosen when
/// climate-specific keys are present.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub enum AttributeSet {
    /// Boxed: much larger than the generic attributes most entities have.
    Climate(Box<ClimateAttributes>),
    Generic(GenericAttributes),
}

impl AttributeSet {
    /// Types `value` by the domain of `entity_id`. Never fails.
    pub(crate) fn for_entity(entity_id: &str, value: Value) -> Self {
        match Domain::of_entity(entity_id) {
            Some(Domain::Climate) => match ClimateAttributes::deserialize(&value) {
                Ok(attrs) => AttributeSet::Climate(Box::new(attrs)),
                Err(_) => Self::generic(value),
            },
            Some(_) => Self::generic(value),
            None => Self::deserialize(value).unwrap_or_default(),
        }
    }

    fn generic(value: Value) -> Self {
        AttributeSet::Generic(lenient_generic(value))
    }

    /// The entity's `friendly_name`, if set.
    pub fn friendly_name(&self) -> Option<&str> {
        match self {
            AttributeSet::Climate(a) => a.friendly_name.as_deref(),
            AttributeSet::Generic(a) => a.friendly_name.as_deref(),
        }
    }

    /// An attribute by its Home Assistant key, as raw JSON, whichever variant holds it.
    pub fn get(&self, key: &str) -> Option<Value> {
        let value = match self {
            AttributeSet::Climate(a) => a.rest.get(key).cloned().or_else(|| a.typed(key)),
            AttributeSet::Generic(a) => a.rest.get(key).cloned().or_else(|| a.typed(key)),
        };
        value.filter(|v| !v.is_null())
    }
}

/// Serializes one typed field by its Home Assistant key, without serializing the rest.
macro_rules! typed_fields {
    ($ty:ty { $($field:ident),* $(,)? }) => {
        impl $ty {
            fn typed(&self, key: &str) -> Option<Value> {
                match key {
                    $( stringify!($field) => serde_json::to_value(&self.$field).ok(), )*
                    _ => None,
                }
            }
        }
    };
}

typed_fields!(ClimateAttributes {
    friendly_name, hvac_modes, hvac_action, current_temperature, current_humidity, temperature,
    target_temp_low, target_temp_high, target_temp_step, min_temp, max_temp, fan_mode, fan_modes,
    preset_mode, preset_modes, swing_mode, swing_modes,
});
typed_fields!(GenericAttributes { friendly_name, unit_of_measurement, device_class, state_class });

/// Parses `GenericAttributes`, never failing: if a known key has an unexpected type,
/// the typed fields are left empty and every key is kept in `rest`.
fn lenient_generic(value: Value) -> GenericAttributes {
    match GenericAttributes::deserialize(&value) {
        Ok(attrs) => attrs,
        Err(e) => {
            log::debug!("Attributes did not match the typed schema ({e}); keeping them untyped.");
            let mut attrs = GenericAttributes::default();
            if let Value::Object(map) = value {
                attrs.rest = map;
            }
            attrs
        }
    }
}

/// A list of a closed enum, skipping values it does not know (a newer Home Assistant).
fn known_values<'de, D, T>(deserializer: D) -> Result<Option<Vec<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: for<'a> Deserialize<'a>,
{
    let values = Option::<Vec<Value>>::deserialize(deserializer)?;
    Ok(values.map(|values| values.into_iter().filter_map(|v| T::deserialize(v).ok()).collect()))
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
                    return Ok(AttributeSet::Climate(Box::new(climate_attrs)));
                }
            }
        }

        Ok(Self::generic(value))
    }
}

impl Default for AttributeSet {
    fn default() -> Self {
        AttributeSet::Generic(GenericAttributes::default())
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

    // Shape captured from a live ESPHome thermostat (name anonymised).
    #[test]
    fn climate_attributes_are_typed() {
        let a = AttributeSet::for_entity("climate.heater", json!({
            "hvac_modes": ["off", "heat", "future_mode"], "min_temp": 10.0, "max_temp": 50.0,
            "target_temp_step": 0.5, "current_temperature": 21.5, "temperature": 23.0,
            "hvac_action": "heating", "friendly_name": "Heater", "supported_features": 385
        }));
        let AttributeSet::Climate(c) = &a else { panic!("expected climate: {a:?}") };
        assert_eq!(c.hvac_modes, Some(vec![HvacMode::Off, HvacMode::Heat]));
        assert_eq!(c.hvac_action, Some(HvacAction::Heating));
        assert_eq!(c.friendly_name.as_deref(), Some("Heater"));
        assert_eq!(c.target_temp_step, Some(0.5));
        assert_eq!(a.get("supported_features"), Some(json!(385)));
        assert_eq!(a.get("temperature"), Some(json!(23.0)));
        assert_eq!(a.get("target_temp_low"), None);
        assert!(!c.rest.contains_key("friendly_name"));
    }

    #[test]
    fn get_covers_every_typed_field() {
        let climate = ClimateAttributes { friendly_name: Some("x".into()), ..Default::default() };
        let generic = GenericAttributes { friendly_name: Some("x".into()), ..Default::default() };
        for set in [AttributeSet::Climate(Box::new(climate)), AttributeSet::Generic(generic)] {
            let Value::Object(all) = serde_json::to_value(&set).unwrap()[match set {
                AttributeSet::Climate(_) => "Climate",
                AttributeSet::Generic(_) => "Generic",
            }]
            .take() else { panic!() };
            for key in all.keys() {
                let typed = match &set {
                    AttributeSet::Climate(a) => a.typed(key),
                    AttributeSet::Generic(a) => a.typed(key),
                };
                assert!(typed.is_some(), "typed_fields! misses `{key}`");
            }
        }
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
