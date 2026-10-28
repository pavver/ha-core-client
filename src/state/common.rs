use super::string_enum;

string_enum! {
    /// Unit of measurement of a sensor. Units not listed here are kept in `Other`.
    pub enum UnitOfMeasurement {
        // Electrical
        Volts => "V",
        Millivolts => "mV",
        Amperes => "A",
        Milliamperes => "mA",
        Watts => "W",
        Kilowatts => "kW",
        WattHours => "Wh",
        KilowattHours => "kWh",
        Ohms => "Ω",

        // Temperature
        Celsius => "°C",
        Fahrenheit => "°F",

        // Time
        Seconds => "s",
        Minutes => "min",
        Hours => "h",

        // Other
        Percentage => "%",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn power_and_energy_are_distinct() {
        assert_eq!(UnitOfMeasurement::from("kW"), UnitOfMeasurement::Kilowatts);
        assert_eq!(UnitOfMeasurement::from("kWh"), UnitOfMeasurement::KilowattHours);
        assert_eq!(UnitOfMeasurement::from("г/м³"), UnitOfMeasurement::Other("г/м³".into()));
        assert_eq!(serde_json::to_value(UnitOfMeasurement::Celsius).unwrap(), "°C");
    }
}
