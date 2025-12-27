pub mod alarm;
pub mod climate;
pub mod common;
pub mod cover;
pub mod lock;

use self::{
    alarm::AlarmState, climate::HvacMode, cover::CoverState, lock::LockState,
};
use serde_json::Value;
use std::fmt;

/// Represents the state of a Home Assistant entity in a strongly-typed way.
#[derive(Debug, PartialEq, Clone)]
pub enum HaState {
    // General
    On,
    Off,
    Unavailable,
    Unknown,

    // Person/Device Tracker
    Home,
    NotHome,

    // Domain-specific states
    Alarm(AlarmState),
    Climate(HvacMode),
    Cover(CoverState),
    Lock(LockState),

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
        let s_lower = s.to_lowercase();
        match s_lower.as_str() {
            // General
            "on" => HaState::On,
            "off" => HaState::Off,
            "unavailable" => HaState::Unavailable,
            "unknown" => HaState::Unknown,
            "open" => HaState::Cover(CoverState::Open), // Also used by Cover
            "closed" => HaState::Cover(CoverState::Closed), // Also used by Cover

            // Person/Device Tracker
            "home" => HaState::Home,
            "not_home" => HaState::NotHome,

            _ => {
                // Check domain-specific states before falling back to generic types
                if let Some(state) = LockState::from_str(s) {
                    return HaState::Lock(state);
                }
                if let Some(state) = AlarmState::from_str(s) {
                    return HaState::Alarm(state);
                }
                if let Some(state) = CoverState::from_str(s) {
                    return HaState::Cover(state);
                }
				//This can be done with a macro in future
                if let Ok(state) = serde_json::from_str::<HvacMode>(&format!("\"{}\"", s)) {
                    return HaState::Climate(state);
                }


                // Fallback to generic types
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
        match self {
            // General
            HaState::On => write!(f, "on"),
            HaState::Off => write!(f, "off"),
            HaState::Unavailable => write!(f, "unavailable"),
            HaState::Unknown => write!(f, "unknown"),

            // Person/Device Tracker
            HaState::Home => write!(f, "home"),
            HaState::NotHome => write!(f, "not_home"),

            // Domain-specific
            HaState::Alarm(state) => write!(f, "{}", state),
            HaState::Climate(state) => write!(f, "{}", state.to_ha_string()),
            HaState::Cover(state) => write!(f, "{}", state),
            HaState::Lock(state) => write!(f, "{}", state),
            
            // Special types
            HaState::Number(n) => write!(f, "{}", n),
            HaState::String(s) => write!(f, "{}", s),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Domain {
    InputNumber,
    HomeAssistant,
    InputText,
    InputSelect,
    Climate,
    AlarmControlPanel,
    Lock,
    Cover,
}

impl Domain {
    pub fn to_str(&self) -> &'static str {
        match self {
            Domain::InputNumber => "input_number",
            Domain::HomeAssistant => "homeassistant",
            Domain::InputText => "input_text",
            Domain::InputSelect => "input_select",
            Domain::Climate => "climate",
            Domain::AlarmControlPanel => "alarm_control_panel",
            Domain::Lock => "lock",
            Domain::Cover => "cover",
        }
    }
}

impl fmt::Display for Domain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_str())
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Service {
    SetValue,
    TurnOn,
    TurnOff,
    Toggle,
    SelectOption,
    SetHvacMode,
    SetTemperature,
    SetFanMode,
    SetPresetMode,
    SetSwingMode,
    AlarmDisarm,
    AlarmArmHome,
    AlarmArmAway,
    AlarmArmNight,
    AlarmArmVacation,
    Lock,
    Unlock,
    OpenCover,
    CloseCover,
    StopCover,
    SetCoverPosition,
    SetCoverTiltPosition,
}

impl Service {
    pub fn to_str(&self) -> &'static str {
        match self {
            Service::SetValue => "set_value",
            Service::TurnOn => "turn_on",
            Service::TurnOff => "turn_off",
            Service::Toggle => "toggle",
            Service::SelectOption => "select_option",
            Service::SetHvacMode => "set_hvac_mode",
            Service::SetTemperature => "set_temperature",
            Service::SetFanMode => "set_fan_mode",
            Service::SetPresetMode => "set_preset_mode",
            Service::SetSwingMode => "set_swing_mode",
            Service::AlarmDisarm => "alarm_disarm",
            Service::AlarmArmHome => "alarm_arm_home",
            Service::AlarmArmAway => "alarm_arm_away",
            Service::AlarmArmNight => "alarm_arm_night",
            Service::AlarmArmVacation => "alarm_arm_vacation",
            Service::Lock => "lock",
            Service::Unlock => "unlock",
            Service::OpenCover => "open_cover",
            Service::CloseCover => "close_cover",
            Service::StopCover => "stop_cover",
            Service::SetCoverPosition => "set_cover_position",
            Service::SetCoverTiltPosition => "set_cover_tilt_position",
        }
    }
}

impl fmt::Display for Service {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_str())
    }
}
