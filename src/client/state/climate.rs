use serde::Deserialize;

/// Represents the possible HVAC modes for a climate entity.
#[derive(Debug, PartialEq, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HvacMode {
    Off,
    Heat,
    Cool,
    HeatCool,
    Auto,
    Dry,
    FanOnly,
}

impl HvacMode {
    /// Converts the enum variant to the string representation Home Assistant expects.
    pub fn to_ha_string(&self) -> &'static str {
        match self {
            HvacMode::Off => "off",
            HvacMode::Heat => "heat",
            HvacMode::Cool => "cool",
            HvacMode::HeatCool => "heat_cool",
            HvacMode::Auto => "auto",
            HvacMode::Dry => "dry",
            HvacMode::FanOnly => "fan_only",
        }
    }
}

/// Represents the fan modes for a climate entity.
#[derive(Debug, PartialEq, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FanMode {
    Auto,
    Low,
    Medium,
    High,
}

impl FanMode {
    pub fn to_ha_string(&self) -> &'static str {
        match self {
            FanMode::Auto => "auto",
            FanMode::Low => "low",
            FanMode::Medium => "medium",
            FanMode::High => "high",
        }
    }
}

/// Represents the swing modes for a climate entity.
#[derive(Debug, PartialEq, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwingMode {
    On,
    Off,
    Vertical,
    Horizontal,
    Both,
}

impl SwingMode {
    pub fn to_ha_string(&self) -> &'static str {
        match self {
            SwingMode::On => "on",
            SwingMode::Off => "off",
            SwingMode::Vertical => "vertical",
            SwingMode::Horizontal => "horizontal",
            SwingMode::Both => "both",
        }
    }
}
/// Represents common preset modes, with a fallback for custom presets.
#[derive(Debug, PartialEq, Clone)]
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

impl PresetMode {
    pub fn to_ha_string(&self) -> String {
        match self {
            PresetMode::Eco => "eco".to_string(),
            PresetMode::Away => "away".to_string(),
            PresetMode::Boost => "boost".to_string(),
            PresetMode::Comfort => "comfort".to_string(),
            PresetMode::Home => "home".to_string(),
            PresetMode::Sleep => "sleep".to_string(),
            PresetMode::Activity => "activity".to_string(),
            PresetMode::Custom(custom) => custom.clone(),
        }
    }
}
