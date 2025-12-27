use serde::Deserialize;
use std::fmt;

#[derive(Debug, PartialEq, Clone)]
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
        D: serde::Deserializer<'de>,
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

impl fmt::Display for UnitOfMeasurement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            UnitOfMeasurement::Volts => "V",
            UnitOfMeasurement::Amperes => "A",
            UnitOfMeasurement::Watts => "W",
            UnitOfMeasurement::KilowattHours => "kWh",
            UnitOfMeasurement::Celsius => "°C",
            UnitOfMeasurement::Fahrenheit => "°F",
            UnitOfMeasurement::Percentage => "%",
            UnitOfMeasurement::Custom(s) => s.as_str(),
        };
        write!(f, "{}", text)
    }
}
