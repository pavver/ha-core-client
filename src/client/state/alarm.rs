use std::fmt;

/// Represents the state of an Alarm Control Panel entity.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum AlarmState {
    Disarmed,
    ArmedHome,
    ArmedAway,
    ArmedNight,
    ArmedVacation,
    Triggered,
}

impl AlarmState {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "disarmed" => Some(AlarmState::Disarmed),
            "armed_home" => Some(AlarmState::ArmedHome),
            "armed_away" => Some(AlarmState::ArmedAway),
            "armed_night" => Some(AlarmState::ArmedNight),
            "armed_vacation" => Some(AlarmState::ArmedVacation),
            "triggered" => Some(AlarmState::Triggered),
            _ => None,
        }
    }

    pub fn to_str(&self) -> &'static str {
        match self {
            AlarmState::Disarmed => "disarmed",
            AlarmState::ArmedHome => "armed_home",
            AlarmState::ArmedAway => "armed_away",
            AlarmState::ArmedNight => "armed_night",
            AlarmState::ArmedVacation => "armed_vacation",
            AlarmState::Triggered => "triggered",
        }
    }
}

impl fmt::Display for AlarmState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_str())
    }
}
