use crate::state::HaState;
use chrono::TimeDelta;
use serde::Serialize;
use std::time::Duration;

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "trigger")]
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
    #[serde(serialize_with = "ha_duration::serialize_opt")]
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
    #[serde(serialize_with = "ha_duration::serialize_opt")]
    pub r#for: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attribute: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
pub struct SunTrigger {
    pub event: SunEvent,
    /// Offset from the sun event; negative values fire before it
    /// (e.g. `TimeDelta::minutes(-30)` is 30 minutes before sunset).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "ha_duration::serialize_opt_signed")]
    pub offset: Option<TimeDelta>,
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
    #[serde(serialize_with = "ha_duration::serialize_opt")]
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

macro_rules! into_trigger {
    ($($platform:ident => $variant:ident),* $(,)?) => {
        $(impl From<$platform> for Trigger {
            fn from(t: $platform) -> Self {
                Trigger::$variant(t)
            }
        })*
    };
}

into_trigger! {
    StateTrigger => State,
    NumericStateTrigger => NumericState,
    SunTrigger => Sun,
    TimeTrigger => Time,
    TimePatternTrigger => TimePattern,
    TemplateTrigger => Template,
    ZoneTrigger => Zone,
}

impl StateTrigger {
    /// Fires on every change of `entity_id` (state or attributes) until narrowed down.
    pub fn new(entity_id: impl Into<String>) -> Self {
        Self { entity_id: entity_id.into(), to: None, from: None, r#for: None, attribute: None }
    }

    pub fn to(mut self, state: impl Into<HaState>) -> Self {
        self.to = Some(state.into());
        self
    }

    pub fn from(mut self, state: impl Into<HaState>) -> Self {
        self.from = Some(state.into());
        self
    }

    /// Fires only after the new state has held for `duration`.
    pub fn for_duration(mut self, duration: Duration) -> Self {
        self.r#for = Some(duration);
        self
    }

    /// Watches this attribute instead of the state.
    pub fn attribute(mut self, attribute: impl Into<String>) -> Self {
        self.attribute = Some(attribute.into());
        self
    }
}

impl NumericStateTrigger {
    /// Needs [`above`](Self::above) and/or [`below`](Self::below).
    pub fn new(entity_id: impl Into<String>) -> Self {
        Self { entity_id: entity_id.into(), above: None, below: None, r#for: None, attribute: None }
    }

    pub fn above(mut self, value: f64) -> Self {
        self.above = Some(value);
        self
    }

    pub fn below(mut self, value: f64) -> Self {
        self.below = Some(value);
        self
    }

    pub fn for_duration(mut self, duration: Duration) -> Self {
        self.r#for = Some(duration);
        self
    }

    pub fn attribute(mut self, attribute: impl Into<String>) -> Self {
        self.attribute = Some(attribute.into());
        self
    }
}

impl SunTrigger {
    pub fn new(event: SunEvent) -> Self {
        Self { event, offset: None }
    }

    /// Negative values fire before the sun event.
    pub fn offset(mut self, offset: TimeDelta) -> Self {
        self.offset = Some(offset);
        self
    }
}

impl TimeTrigger {
    /// `at` is a time like `"07:30:00"` or an `input_datetime` entity id.
    pub fn new(at: impl Into<String>) -> Self {
        Self { at: at.into() }
    }
}

impl TimePatternTrigger {
    /// Starts empty; set at least one of hours, minutes, seconds (e.g. `minutes("/5")`).
    pub fn new() -> Self {
        Self::default()
    }

    pub fn hours(mut self, pattern: impl Into<String>) -> Self {
        self.hours = Some(pattern.into());
        self
    }

    pub fn minutes(mut self, pattern: impl Into<String>) -> Self {
        self.minutes = Some(pattern.into());
        self
    }

    pub fn seconds(mut self, pattern: impl Into<String>) -> Self {
        self.seconds = Some(pattern.into());
        self
    }
}

impl TemplateTrigger {
    pub fn new(value_template: impl Into<String>) -> Self {
        Self { value_template: value_template.into(), r#for: None }
    }

    pub fn for_duration(mut self, duration: Duration) -> Self {
        self.r#for = Some(duration);
        self
    }
}

impl ZoneTrigger {
    pub fn new(entity_id: impl Into<String>, zone: impl Into<String>, event: ZoneEvent) -> Self {
        Self { entity_id: entity_id.into(), zone: zone.into(), event }
    }
}

/// Home Assistant time periods: `HH:MM:SS` or `HH:MM:SS.ffffff`, optionally negative.
mod ha_duration {
    use super::*;
    use serde::Serializer;

    pub(super) fn format(negative: bool, secs: u64, micros: u32) -> String {
        let sign = if negative { "-" } else { "" };
        let (h, m, s) = (secs / 3600, secs / 60 % 60, secs % 60);
        if micros == 0 {
            format!("{sign}{h:02}:{m:02}:{s:02}")
        } else {
            format!("{sign}{h:02}:{m:02}:{s:02}.{micros:06}")
        }
    }

    pub fn serialize_opt<S: Serializer>(d: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
        match d {
            Some(d) => s.serialize_str(&format(false, d.as_secs(), d.subsec_micros())),
            None => s.serialize_none(),
        }
    }

    pub fn serialize_opt_signed<S: Serializer>(d: &Option<TimeDelta>, s: S) -> Result<S::Ok, S::Error> {
        match d {
            Some(d) => {
                let abs = d.abs();
                let micros = (abs.subsec_nanos() / 1000) as u32;
                s.serialize_str(&format(*d < TimeDelta::zero(), abs.num_seconds() as u64, micros))
            }
            None => s.serialize_none(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::lock::LockState;
    use serde_json::json;

    #[test]
    fn state_trigger_wire_format() {
        let t = Trigger::State(StateTrigger {
            entity_id: "lock.front".into(),
            to: Some(HaState::On),
            from: Some(HaState::Lock(LockState::Locked)),
            r#for: Some(Duration::from_secs(90)),
            attribute: None,
        });
        assert_eq!(
            serde_json::to_value(t).unwrap(),
            json!({"trigger": "state", "entity_id": "lock.front", "to": "on", "from": "locked", "for": "00:01:30"})
        );
    }

    #[test]
    fn builders_match_struct_literals() {
        let built: Trigger = StateTrigger::new("lock.front")
            .to(HaState::On)
            .from(HaState::Lock(LockState::Locked))
            .for_duration(Duration::from_secs(90))
            .into();
        assert_eq!(
            serde_json::to_value(built).unwrap(),
            json!({"trigger": "state", "entity_id": "lock.front", "to": "on", "from": "locked", "for": "00:01:30"})
        );
        let t: Trigger = NumericStateTrigger::new("sensor.t").above(20.5).attribute("temperature").into();
        assert_eq!(
            serde_json::to_value(t).unwrap(),
            json!({"trigger": "numeric_state", "entity_id": "sensor.t", "above": 20.5, "attribute": "temperature"})
        );
        let t: Trigger = TimePatternTrigger::new().minutes("/5").into();
        assert_eq!(serde_json::to_value(t).unwrap(), json!({"trigger": "time_pattern", "minutes": "/5"}));
    }

    #[test]
    fn durations() {
        let t = Trigger::Template(TemplateTrigger {
            value_template: "{{ true }}".into(),
            r#for: Some(Duration::from_millis(100_500_250)),
        });
        assert_eq!(serde_json::to_value(t).unwrap()["for"], "27:55:00.250000");

        let sun = |offset| serde_json::to_value(Trigger::Sun(SunTrigger { event: SunEvent::Sunset, offset })).unwrap();
        assert_eq!(sun(Some(TimeDelta::minutes(-30)))["offset"], "-00:30:00");
        assert_eq!(sun(Some(TimeDelta::seconds(3661)))["offset"], "01:01:01");
        assert!(sun(None).get("offset").is_none());
    }
}
