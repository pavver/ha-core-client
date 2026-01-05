use std::collections::HashMap;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use crate::client::state::climate::{FanMode, HvacMode, PresetMode, SwingMode};
use crate::client::state::common::UnitOfMeasurement;
use crate::client::state::device_class::DeviceClass;
use crate::client::state::state_class::StateClass;

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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GenericAttributes {
    pub friendly_name: Option<String>,
    pub unit_of_measurement: Option<UnitOfMeasurement>,
    pub device_class: Option<DeviceClass>,
    pub state_class: Option<StateClass>,
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HistoricalClimateAttributes {
    pub friendly_name: Option<String>,
    pub current_temperature: Option<f64>,
    pub temperature: Option<f64>,
    #[serde(flatten)]
    pub rest: HashMap<String, Value>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
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
