use serde::{Deserialize, Serialize};
use strum::{EnumString, Display, AsRefStr};

/// Represents the possible HVAC modes for a climate entity.
#[derive(Debug, PartialEq, Clone, Copy, Deserialize, EnumString, Display, AsRefStr, Serialize)]
#[serde(rename_all = "snake_case")] // Keep for serde deserialization
#[strum(serialize_all = "snake_case", ascii_case_insensitive)] // Add for strum EnumString and Display
pub enum HvacMode {
    Off,
    Heat,
    Cool,
    HeatCool,
    Auto,
    Dry,
    FanOnly,
}

/// Represents the fan modes for a climate entity.
#[derive(Debug, PartialEq, Clone, Copy, Deserialize, EnumString, Display, AsRefStr, Serialize)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case", ascii_case_insensitive)]
pub enum FanMode {
    Auto,
    Low,
    Medium,
    High,
}

/// Represents the swing modes for a climate entity.
#[derive(Debug, PartialEq, Clone, Copy, Deserialize, EnumString, Display, AsRefStr, Serialize)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case", ascii_case_insensitive)]
pub enum SwingMode {
    On,
    Off,
    Vertical,
    Horizontal,
    Both,
}
/// Represents common preset modes, with a fallback for custom presets.
#[derive(Debug, PartialEq, Clone, Display, AsRefStr)]
pub enum PresetMode {
    Eco,
    Away,
    Boost,
    Comfort,
    Home,
    Sleep,
    Activity,
    Custom(String),
}

impl<'de> Deserialize<'de> for PresetMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(match s.as_str() {
            "eco" => PresetMode::Eco,
            "away" => PresetMode::Away,
            "boost" => PresetMode::Boost,
            "comfort" => PresetMode::Comfort,
            "home" => PresetMode::Home,
            "sleep" => PresetMode::Sleep,
            "activity" => PresetMode::Activity,
            _ => PresetMode::Custom(s),
        })
    }
}

impl Serialize for PresetMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            PresetMode::Eco => serializer.serialize_str("eco"),
            PresetMode::Away => serializer.serialize_str("away"),
            PresetMode::Boost => serializer.serialize_str("boost"),
            PresetMode::Comfort => serializer.serialize_str("comfort"),
            PresetMode::Home => serializer.serialize_str("home"),
            PresetMode::Sleep => serializer.serialize_str("sleep"),
            PresetMode::Activity => serializer.serialize_str("activity"),
            PresetMode::Custom(s) => serializer.serialize_str(s),
        }
    }
}



