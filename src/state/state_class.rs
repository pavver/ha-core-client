use super::string_enum;

string_enum! {
    /// State class of a sensor entity.
    pub enum StateClass {
        Measurement => "measurement",
        MeasurementAngle => "measurement_angle",
        Total => "total",
        TotalIncreasing => "total_increasing",
    }
}
