use serde::{Deserialize, Deserializer, Serialize, Serializer};
use strum::Display;

#[derive(Debug, PartialEq, Clone, Display)]
pub enum UnitOfMeasurement {
    // Electrical
    Volts,
    Amperes,
    Watts,
    KilowattHours,

    // Temperature
    Celsius,
    Fahrenheit,

    // Other
    Percentage,
    Custom(String),
}

impl<'de> Deserialize<'de> for UnitOfMeasurement {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(match s.as_str() {
            "V" => UnitOfMeasurement::Volts,
            "A" => UnitOfMeasurement::Amperes,
            "W" => UnitOfMeasurement::Watts,
            "kW" => UnitOfMeasurement::KilowattHours,
            "°C" | "C" => UnitOfMeasurement::Celsius,
            "°F" | "F" => UnitOfMeasurement::Fahrenheit,
            "%" => UnitOfMeasurement::Percentage,
            _ => UnitOfMeasurement::Custom(s),
        })
    }
}

impl Serialize for UnitOfMeasurement {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match self {
            UnitOfMeasurement::Volts => "V",
            UnitOfMeasurement::Amperes => "A",
            UnitOfMeasurement::Watts => "W",
            UnitOfMeasurement::KilowattHours => "kW",
            UnitOfMeasurement::Celsius => "°C",
            UnitOfMeasurement::Fahrenheit => "°F",
            UnitOfMeasurement::Percentage => "%",
            UnitOfMeasurement::Custom(s) => s,
        })
    }
}


