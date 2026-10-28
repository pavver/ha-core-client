use strum::{EnumString, Display};
use serde::{Deserialize, Serialize};

/// Represents the state of an Alarm Control Panel entity.
#[derive(Debug, PartialEq, Clone, Copy, EnumString, Display, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AlarmState {
    Disarmed,
    ArmedHome,
    ArmedAway,
    ArmedNight,
    ArmedVacation,
    ArmedCustomBypass,
    Arming,
    Disarming,
    Pending,
    Triggered,
}
