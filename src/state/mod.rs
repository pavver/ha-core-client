mod macros;

pub mod alarm;
pub mod climate;
pub mod common;
pub mod cover;
pub mod device_class;
pub mod lock;
pub mod state_class;

pub(crate) use macros::string_enum;

use self::{
    alarm::AlarmState, climate::HvacMode, cover::CoverState, lock::LockState,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::fmt;
use std::str::FromStr;

/// Represents the state of a Home Assistant entity in a strongly-typed way.
///
/// Serializes to the plain state string Home Assistant uses (e.g. `"on"`, `"locked"`),
/// so it can be used directly in triggers.
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

    /// Parses a state using the entity's domain to resolve ambiguous values:
    /// `"open"` is `Lock(Open)` for `lock`, `Cover(Open)` for `cover`; `"heat"` is
    /// `Climate(Heat)` only for `climate`. Other domains fall back to [`from_string`](Self::from_string).
    pub fn parse_for_domain(domain: &Domain, s: &str) -> Self {
        let s_lower = s.to_lowercase();
        let domain_specific = match domain {
            Domain::Lock => LockState::from_str(&s_lower).ok().map(HaState::Lock),
            Domain::Cover => CoverState::from_str(&s_lower).ok().map(HaState::Cover),
            Domain::AlarmControlPanel => AlarmState::from_str(&s_lower).ok().map(HaState::Alarm),
            Domain::Climate => HvacMode::from_str(&s_lower).ok().map(HaState::Climate),
            _ => None,
        };
        domain_specific.unwrap_or_else(|| Self::from_string(s))
    }

    /// Parses a string slice into an HaState without knowing the entity's domain.
    /// Ambiguous values resolve to the first match in this order: cover, lock, alarm,
    /// climate. Prefer [`parse_for_domain`](Self::parse_for_domain) (or
    /// `State::ha_state`) when the domain is known.
    pub fn from_string(s: &str) -> Self {
        let s_lower = s.to_lowercase();
        match s_lower.as_str() {
            // General
            "on" => HaState::On,
            "off" => HaState::Off,
            "unavailable" => HaState::Unavailable,
            "unknown" => HaState::Unknown,

            // Person/Device Tracker
            "home" => HaState::Home,
            "not_home" => HaState::NotHome,

            _ => {
                // Check domain-specific states before falling back to generic types
                if let Ok(state) = CoverState::from_str(&s_lower) {
                    return HaState::Cover(state);
                }
                if let Ok(state) = LockState::from_str(&s_lower) {
                    return HaState::Lock(state);
                }
                if let Ok(state) = AlarmState::from_str(&s_lower) {
                    return HaState::Alarm(state);
                }
                if let Ok(state) = HvacMode::from_str(&s_lower) {
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
            HaState::Climate(state) => write!(f, "{}", state),
            HaState::Cover(state) => write!(f, "{}", state),
            HaState::Lock(state) => write!(f, "{}", state),

            // Special types
            HaState::Number(n) => write!(f, "{}", n),
            HaState::String(s) => write!(f, "{}", s),
        }
    }
}

impl Serialize for HaState {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for HaState {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(HaState::from_value(&Value::deserialize(deserializer)?))
    }
}

impl From<&str> for HaState {
    fn from(s: &str) -> Self {
        HaState::from_string(s)
    }
}

string_enum! {
    /// A Home Assistant integration domain (the part of an entity id before the dot).
    pub enum Domain {
        HomeAssistant => "homeassistant",
        Light => "light",
        Switch => "switch",
        Fan => "fan",
        Climate => "climate",
        Cover => "cover",
        Lock => "lock",
        AlarmControlPanel => "alarm_control_panel",
        MediaPlayer => "media_player",
        InputBoolean => "input_boolean",
        InputNumber => "input_number",
        InputText => "input_text",
        InputSelect => "input_select",
        InputButton => "input_button",
        Button => "button",
        Number => "number",
        Select => "select",
        Script => "script",
        Scene => "scene",
        Automation => "automation",
        Notify => "notify",
    }
}

impl Domain {
    /// Extracts the domain from an entity id such as `light.kitchen`.
    pub fn of_entity(entity_id: &str) -> Option<Domain> {
        match entity_id.split_once('.') {
            Some((domain, object_id)) if !domain.is_empty() && !object_id.is_empty() => {
                Some(Domain::from(domain))
            }
            _ => None,
        }
    }
}

string_enum! {
    /// A Home Assistant service name.
    pub enum Service {
        TurnOn => "turn_on",
        TurnOff => "turn_off",
        Toggle => "toggle",
        SetValue => "set_value",
        SelectOption => "select_option",
        Press => "press",
        Trigger => "trigger",
        Reload => "reload",
        SetHvacMode => "set_hvac_mode",
        SetTemperature => "set_temperature",
        SetFanMode => "set_fan_mode",
        SetPresetMode => "set_preset_mode",
        SetSwingMode => "set_swing_mode",
        AlarmDisarm => "alarm_disarm",
        AlarmArmHome => "alarm_arm_home",
        AlarmArmAway => "alarm_arm_away",
        AlarmArmNight => "alarm_arm_night",
        AlarmArmVacation => "alarm_arm_vacation",
        Lock => "lock",
        Unlock => "unlock",
        Open => "open",
        OpenCover => "open_cover",
        CloseCover => "close_cover",
        StopCover => "stop_cover",
        SetCoverPosition => "set_cover_position",
        SetCoverTiltPosition => "set_cover_tilt_position",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_wire_strings() {
        assert_eq!(Domain::HomeAssistant.to_string(), "homeassistant");
        assert_eq!(Domain::AlarmControlPanel.as_str(), "alarm_control_panel");
        assert_eq!(Domain::from("vacuum"), Domain::Other("vacuum".into()));
        assert_eq!(Domain::Other("vacuum".into()).to_string(), "vacuum");
        assert_eq!(Domain::of_entity("fan.bedroom"), Some(Domain::Fan));
        assert_eq!(Domain::of_entity("nodot"), None);
        assert_eq!(Domain::of_entity(".x"), None);
    }

    #[test]
    fn service_round_trip() {
        assert_eq!(serde_json::to_value(Service::SetHvacMode).unwrap(), "set_hvac_mode");
        let s: Service = serde_json::from_value("start".into()).unwrap();
        assert_eq!(s, Service::Other("start".into()));
    }

    #[test]
    fn ha_state_serializes_as_plain_string() {
        let v = serde_json::to_value([
            HaState::On,
            HaState::NotHome,
            HaState::Lock(LockState::Locked),
            HaState::Alarm(AlarmState::ArmedAway),
            HaState::Climate(HvacMode::HeatCool),
            HaState::Number(21.0),
            HaState::Number(21.5),
            HaState::String("custom".into()),
        ])
        .unwrap();
        assert_eq!(
            v,
            serde_json::json!(["on", "not_home", "locked", "armed_away", "heat_cool", "21", "21.5", "custom"])
        );
    }

    #[test]
    fn ha_state_parsing() {
        assert_eq!(HaState::from_string("ON"), HaState::On);
        assert_eq!(HaState::from_string("Locked"), HaState::Lock(LockState::Locked));
        assert_eq!(HaState::from_string("open"), HaState::Cover(CoverState::Open));
        assert_eq!(HaState::from_string("12.5"), HaState::Number(12.5));
        assert_eq!(HaState::from_string("Mixed"), HaState::String("Mixed".into()));
    }

    #[test]
    fn domain_resolves_ambiguous_states() {
        assert_eq!(HaState::parse_for_domain(&Domain::Lock, "open"), HaState::Lock(LockState::Open));
        assert_eq!(HaState::parse_for_domain(&Domain::Cover, "open"), HaState::Cover(CoverState::Open));
        assert_eq!(HaState::parse_for_domain(&Domain::Climate, "heat"), HaState::Climate(HvacMode::Heat));
        assert_eq!(HaState::parse_for_domain(&Domain::Climate, "off"), HaState::Climate(HvacMode::Off));
        assert_eq!(HaState::parse_for_domain(&Domain::Light, "off"), HaState::Off);
        assert_eq!(HaState::parse_for_domain(&Domain::Other("sensor".into()), "21.5"), HaState::Number(21.5));
        assert_eq!(HaState::parse_for_domain(&Domain::Lock, "unavailable"), HaState::Unavailable);
    }
}
