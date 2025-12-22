use serde_json::Value;
use std::fmt;

/// Represents the possible HVAC modes for a climate entity.
#[derive(Debug, PartialEq, Clone, Copy)]
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
#[derive(Debug, PartialEq, Clone, Copy)]
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
#[derive(Debug, PartialEq, Clone, Copy)]
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

/// Represents the state of a Home Assistant entity in a strongly-typed way.
#[derive(Debug, PartialEq, Clone)]
pub enum HaState {
    // General
    On,
    Off,
    Unavailable,
    Unknown,
    Open,
    Closed,
    
    // Lock
    Locked,
    Unlocked,
    Locking,
    Unlocking,
    Jammed,

    // Climate
    Heat,
    Cool,
    HeatCool,
    Auto,
    Dry,
    FanOnly,

    // Person/Device Tracker
    Home,
    NotHome,

    // Alarm Control Panel
    Disarmed,
    ArmedHome,
    ArmedAway,
    ArmedNight,
    ArmedVacation,
    Triggered,

    // Covers
    Opening,
    Closing,

    // Special types
    Number(f64),
    String(String),
}

impl HaState {
    /// Parses a serde_json::Value (which is expected to be a string) into an HaState.
    pub fn from_value(value: &Value) -> Self {
        let s = match value.as_str() {
            Some(s) => s,
            // If the value is not a string (e.g., a number), convert it to a string and then parse.
            None => return Self::from_string(&value.to_string()),
        };
        Self::from_string(s)
    }

    /// Parses a string slice into an HaState.
    pub fn from_string(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            // General
            "on" => HaState::On,
            "off" => HaState::Off,
            "unavailable" => HaState::Unavailable,
            "unknown" => HaState::Unknown,
            "open" => HaState::Open,
            "closed" => HaState::Closed,

            // Lock
            "locked" => HaState::Locked,
            "unlocked" => HaState::Unlocked,
            "locking" => HaState::Locking,
            "unlocking" => HaState::Unlocking,
            "jammed" => HaState::Jammed,

            // Climate
            "heat" => HaState::Heat,
            "cool" => HaState::Cool,
            "heat_cool" => HaState::HeatCool,
            "auto" => HaState::Auto,
            "dry" => HaState::Dry,
            "fan_only" => HaState::FanOnly,

            // Person/Device Tracker
            "home" => HaState::Home,
            "not_home" => HaState::NotHome,

            // Alarm Control Panel
            "disarmed" => HaState::Disarmed,
            "armed_home" => HaState::ArmedHome,
            "armed_away" => HaState::ArmedAway,
            "armed_night" => HaState::ArmedNight,
            "armed_vacation" => HaState::ArmedVacation,
            "triggered" => HaState::Triggered,

            // Covers
            "opening" => HaState::Opening,
            "closing" => HaState::Closing,
            
            _ => {
                if let Ok(num) = s.parse::<f64>() {
                    HaState::Number(num)
                } else {
                    HaState::String(s.to_string())
                }
            }
        }
    }
}

/// Allows the enum to be printed out nicely, which is useful for logging.
impl fmt::Display for HaState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            HaState::On => "on",
            HaState::Off => "off",
            HaState::Unavailable => "unavailable",
            HaState::Unknown => "unknown",
            HaState::Open => "open",
            HaState::Closed => "closed",
            HaState::Locked => "locked",
            HaState::Unlocked => "unlocked",
            HaState::Locking => "locking",
            HaState::Unlocking => "unlocking",
            HaState::Jammed => "jammed",
            HaState::Heat => "heat",
            HaState::Cool => "cool",
            HaState::HeatCool => "heat_cool",
            HaState::Auto => "auto",
            HaState::Dry => "dry",
            HaState::FanOnly => "fan_only",
            HaState::Home => "home",
            HaState::NotHome => "not_home",
            HaState::Disarmed => "disarmed",
            HaState::ArmedHome => "armed_home",
            HaState::ArmedAway => "armed_away",
            HaState::ArmedNight => "armed_night",
            HaState::ArmedVacation => "armed_vacation",
            HaState::Triggered => "triggered",
            HaState::Opening => "opening",
            HaState::Closing => "closing",
            HaState::Number(n) => return write!(f, "{}", n),
            HaState::String(s) => return write!(f, "{}", s),
        };
        write!(f, "{}", text)
    }
}
