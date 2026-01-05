use strum::{EnumString, Display};
use serde::Serialize;

/// Represents the state of an Alarm Control Panel entity.
#[derive(Debug, PartialEq, Clone, Copy, EnumString, Display, Serialize)]
#[strum(serialize_all = "snake_case")]
pub enum AlarmState {
    Disarmed,
    ArmedHome,
    ArmedAway,
    ArmedNight,
    ArmedVacation,
    Triggered,
}
