use serde::{Deserialize, Serialize};
use strum::{EnumString, Display, AsRefStr};

use super::string_enum;

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

string_enum! {
    /// Fan modes of a climate entity. Integrations often define their own
    /// (`quiet`, `turbo`, …), which end up in `Other`.
    pub enum FanMode {
        On => "on",
        Off => "off",
        Auto => "auto",
        Low => "low",
        Medium => "medium",
        Middle => "middle",
        High => "high",
        Focus => "focus",
        Diffuse => "diffuse",
        Top => "top",
    }
}

string_enum! {
    /// Swing modes of a climate entity.
    pub enum SwingMode {
        On => "on",
        Off => "off",
        Vertical => "vertical",
        Horizontal => "horizontal",
        Both => "both",
    }
}

string_enum! {
    /// Preset modes of a climate entity, with a fallback for custom presets.
    pub enum PresetMode {
        None => "none",
        Eco => "eco",
        Away => "away",
        Boost => "boost",
        Comfort => "comfort",
        Home => "home",
        Sleep => "sleep",
        Activity => "activity",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modes_use_wire_strings() {
        assert_eq!(PresetMode::Eco.to_string(), "eco");
        assert_eq!(PresetMode::from("my_preset").to_string(), "my_preset");
        let fans: Vec<FanMode> = serde_json::from_str(r#"["auto","quiet","turbo"]"#).unwrap();
        assert_eq!(fans, vec![FanMode::Auto, FanMode::Other("quiet".into()), FanMode::Other("turbo".into())]);
    }
}
