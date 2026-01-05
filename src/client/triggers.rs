use crate::client::state::HaState;
use serde::Serialize;
use std::time::Duration;

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "platform")]
pub enum Trigger {
    State(StateTrigger),
    NumericState(NumericStateTrigger),
    Sun(SunTrigger),
    Time(TimeTrigger),
    TimePattern(TimePatternTrigger),
    Template(TemplateTrigger),
    Zone(ZoneTrigger),
}

#[derive(Serialize, Debug, Clone)]
pub struct StateTrigger {
    pub entity_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<HaState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<HaState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    pub r#for: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attribute: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
pub struct NumericStateTrigger {
    pub entity_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub above: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub below: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    pub r#for: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attribute: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
pub struct SunTrigger {
    pub event: SunEvent,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    pub offset: Option<Duration>,
}

#[derive(Serialize, Debug, strum::AsRefStr, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum SunEvent {
    Sunrise,
    Sunset,
}

#[derive(Serialize, Debug, Clone)]
pub struct TimeTrigger {
    pub at: String,
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct TimePatternTrigger {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hours: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minutes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seconds: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
pub struct TemplateTrigger {
    pub value_template: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    pub r#for: Option<Duration>,
}

#[derive(Serialize, Debug, Clone)]
pub struct ZoneTrigger {
    pub entity_id: String,
    pub zone: String,
    pub event: ZoneEvent,
}

#[derive(Serialize, Debug, strum::AsRefStr, Default, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ZoneEvent {
    #[default]
    Enter,
    Leave,
}