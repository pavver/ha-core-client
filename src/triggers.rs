use crate::state::HaState;
use chrono::TimeDelta;
use serde::{Serialize, Serializer};
use serde_json::{Map, Value};
use std::time::Duration;

/// One trigger of any supported platform. Build it from a platform struct (`.into()`).
///
/// The platform structs are `#[non_exhaustive]`: construct them with `new` and the builder
/// methods. Every platform has an [`id`](StateTrigger::id) to tell triggers apart when
/// several are subscribed together (see [`TriggerFired::id`](crate::TriggerFired::id)).
#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "trigger")]
#[non_exhaustive]
pub enum Trigger {
    State(StateTrigger),
    NumericState(NumericStateTrigger),
    Sun(SunTrigger),
    Time(TimeTrigger),
    TimePattern(TimePatternTrigger),
    Template(TemplateTrigger),
    Zone(ZoneTrigger),
    Event(EventTrigger),
    #[serde(rename = "homeassistant")]
    HomeAssistant(HomeAssistantTrigger),
    Mqtt(MqttTrigger),
    Webhook(WebhookTrigger),
    Calendar(CalendarTrigger),
    Tag(TagTrigger),
    Conversation(ConversationTrigger),
}

/// What a `to`/`from` option of a [`StateTrigger`] matches.
#[derive(Debug, Clone, PartialEq)]
pub enum StateMatch {
    /// Any state, but not attribute-only changes (sent as `null`, `~` in YAML).
    Any,
    /// One state.
    One(HaState),
    /// Any of several states.
    AnyOf(Vec<HaState>),
}

impl Serialize for StateMatch {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            StateMatch::Any => s.serialize_none(),
            StateMatch::One(state) => state.serialize(s),
            StateMatch::AnyOf(states) => states.serialize(s),
        }
    }
}

/// Threshold of a [`NumericStateTrigger`]: a number or the entity id of a numeric entity.
#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum Threshold {
    Value(f64),
    Entity(String),
}

#[derive(Serialize, Debug, Clone)]
#[non_exhaustive]
pub struct StateTrigger {
    pub entity_id: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<StateMatch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<StateMatch>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub not_to: Vec<HaState>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub not_from: Vec<HaState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "ha_duration::serialize_opt")]
    pub r#for: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attribute: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
#[non_exhaustive]
pub struct NumericStateTrigger {
    pub entity_id: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub above: Option<Threshold>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub below: Option<Threshold>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_template: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "ha_duration::serialize_opt")]
    pub r#for: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attribute: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
#[non_exhaustive]
pub struct SunTrigger {
    pub event: SunEvent,
    /// Offset from the sun event; negative values fire before it
    /// (e.g. `TimeDelta::minutes(-30)` is 30 minutes before sunset).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "ha_duration::serialize_opt_signed")]
    pub offset: Option<TimeDelta>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Serialize, Debug, strum::AsRefStr, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum SunEvent {
    Sunrise,
    Sunset,
}

#[derive(Serialize, Debug, Clone)]
#[non_exhaustive]
pub struct TimeTrigger {
    pub at: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Serialize, Debug, Default, Clone)]
#[non_exhaustive]
pub struct TimePatternTrigger {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hours: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minutes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seconds: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
#[non_exhaustive]
pub struct TemplateTrigger {
    pub value_template: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "ha_duration::serialize_opt")]
    pub r#for: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
#[non_exhaustive]
pub struct ZoneTrigger {
    pub entity_id: String,
    pub zone: String,
    pub event: ZoneEvent,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Serialize, Debug, strum::AsRefStr, Default, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ZoneEvent {
    #[default]
    Enter,
    Leave,
}

/// A bus event of one or several types.
#[derive(Serialize, Debug, Clone)]
#[non_exhaustive]
pub struct EventTrigger {
    pub event_type: Vec<String>,
    /// Data the event must contain.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_data: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
#[non_exhaustive]
pub struct HomeAssistantTrigger {
    pub event: HomeAssistantEvent,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Serialize, Debug, strum::AsRefStr, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum HomeAssistantEvent {
    Start,
    Shutdown,
}

#[derive(Serialize, Debug, Clone)]
#[non_exhaustive]
pub struct MqttTrigger {
    pub topic: String,
    /// The payload a message must have (after `value_template`); any message if unset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_template: Option<String>,
    /// Payload encoding (`utf-8` by default; empty for raw bytes).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qos: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
#[non_exhaustive]
pub struct WebhookTrigger {
    pub webhook_id: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub allowed_methods: Vec<WebhookMethod>,
    /// `false` also accepts requests from outside the local network.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Serialize, Debug, strum::AsRefStr, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
#[strum(serialize_all = "UPPERCASE")]
pub enum WebhookMethod {
    Post,
    Put,
    Get,
    Head,
}

#[derive(Serialize, Debug, Clone)]
#[non_exhaustive]
pub struct CalendarTrigger {
    pub entity_id: String,
    pub event: CalendarEvent,
    /// Negative values fire before the event start/end.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "ha_duration::serialize_opt_signed")]
    pub offset: Option<TimeDelta>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Serialize, Debug, strum::AsRefStr, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum CalendarEvent {
    Start,
    End,
}

#[derive(Serialize, Debug, Clone)]
#[non_exhaustive]
pub struct TagTrigger {
    pub tag_id: Vec<String>,
    /// Only scans by these devices; any device if empty.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub device_id: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

/// A voice/text command (sentence trigger). Sentences may use templates like `[it's ]party time`.
#[derive(Serialize, Debug, Clone)]
#[non_exhaustive]
pub struct ConversationTrigger {
    pub command: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
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
    EventTrigger => Event,
    HomeAssistantTrigger => HomeAssistant,
    MqttTrigger => Mqtt,
    WebhookTrigger => Webhook,
    CalendarTrigger => Calendar,
    TagTrigger => Tag,
    ConversationTrigger => Conversation,
}

macro_rules! trigger_id {
    ($($platform:ident),* $(,)?) => {
        $(impl $platform {
            /// Names the trigger; it comes back as [`TriggerFired::id`](crate::TriggerFired::id).
            pub fn id(mut self, id: impl Into<String>) -> Self {
                self.id = Some(id.into());
                self
            }
        })*
    };
}

trigger_id! {
    StateTrigger, NumericStateTrigger, SunTrigger, TimeTrigger, TimePatternTrigger, TemplateTrigger,
    ZoneTrigger, EventTrigger, HomeAssistantTrigger, MqttTrigger, WebhookTrigger, CalendarTrigger,
    TagTrigger, ConversationTrigger,
}

fn strings<I, S>(items: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    items.into_iter().map(Into::into).collect()
}

fn states<I, S>(items: I) -> Vec<HaState>
where
    I: IntoIterator<Item = S>,
    S: Into<HaState>,
{
    items.into_iter().map(Into::into).collect()
}

impl StateTrigger {
    /// Fires on every change of `entity_id` (state or attributes) until narrowed down.
    pub fn new(entity_id: impl Into<String>) -> Self {
        Self::entities([entity_id.into()])
    }

    /// Watches several entities.
    pub fn entities<I, S>(entity_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            entity_id: strings(entity_ids),
            to: None,
            from: None,
            not_to: Vec::new(),
            not_from: Vec::new(),
            r#for: None,
            attribute: None,
            id: None,
        }
    }

    /// Adds an entity to watch.
    pub fn and_entity(mut self, entity_id: impl Into<String>) -> Self {
        self.entity_id.push(entity_id.into());
        self
    }

    /// Fires when the entity changes to this state.
    pub fn to(mut self, state: impl Into<HaState>) -> Self {
        self.to = Some(StateMatch::One(state.into()));
        self
    }

    /// Fires when the entity changes to any of these states.
    pub fn to_any_of<I, S>(mut self, states_: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<HaState>,
    {
        self.to = Some(StateMatch::AnyOf(states(states_)));
        self
    }

    /// Fires on every state change, but not on attribute-only changes (`to: null`).
    pub fn to_any_state(mut self) -> Self {
        self.to = Some(StateMatch::Any);
        self
    }

    pub fn from(mut self, state: impl Into<HaState>) -> Self {
        self.from = Some(StateMatch::One(state.into()));
        self
    }

    pub fn from_any_of<I, S>(mut self, states_: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<HaState>,
    {
        self.from = Some(StateMatch::AnyOf(states(states_)));
        self
    }

    /// Requires some previous state, ignoring attribute-only changes (`from: null`).
    pub fn from_any_state(mut self) -> Self {
        self.from = Some(StateMatch::Any);
        self
    }

    /// Fires on changes to any state except these.
    pub fn not_to<I, S>(mut self, states_: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<HaState>,
    {
        self.not_to = states(states_);
        self
    }

    /// Fires on changes from any state except these.
    pub fn not_from<I, S>(mut self, states_: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<HaState>,
    {
        self.not_from = states(states_);
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
        Self::entities([entity_id.into()])
    }

    /// Watches several entities.
    pub fn entities<I, S>(entity_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            entity_id: strings(entity_ids),
            above: None,
            below: None,
            value_template: None,
            r#for: None,
            attribute: None,
            id: None,
        }
    }

    /// Adds an entity to watch.
    pub fn and_entity(mut self, entity_id: impl Into<String>) -> Self {
        self.entity_id.push(entity_id.into());
        self
    }

    pub fn above(mut self, value: f64) -> Self {
        self.above = Some(Threshold::Value(value));
        self
    }

    /// The lower threshold is the state of another (numeric) entity.
    pub fn above_entity(mut self, entity_id: impl Into<String>) -> Self {
        self.above = Some(Threshold::Entity(entity_id.into()));
        self
    }

    pub fn below(mut self, value: f64) -> Self {
        self.below = Some(Threshold::Value(value));
        self
    }

    /// The upper threshold is the state of another (numeric) entity.
    pub fn below_entity(mut self, entity_id: impl Into<String>) -> Self {
        self.below = Some(Threshold::Entity(entity_id.into()));
        self
    }

    /// Template that computes the value to compare (instead of the state/attribute).
    pub fn value_template(mut self, template: impl Into<String>) -> Self {
        self.value_template = Some(template.into());
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
        Self { event, offset: None, id: None }
    }

    /// Negative values fire before the sun event.
    pub fn offset(mut self, offset: TimeDelta) -> Self {
        self.offset = Some(offset);
        self
    }
}

impl TimeTrigger {
    /// `at` is a time like `"07:30"` or `"07:30:00"`, an `input_datetime` entity id (a
    /// time-only helper fires daily) or the entity id of a timestamp sensor.
    pub fn new(at: impl Into<String>) -> Self {
        Self { at: vec![at.into()], id: None }
    }

    /// Adds another time (same formats as [`new`](Self::new)).
    pub fn and_at(mut self, at: impl Into<String>) -> Self {
        self.at.push(at.into());
        self
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
        Self { value_template: value_template.into(), r#for: None, id: None }
    }

    pub fn for_duration(mut self, duration: Duration) -> Self {
        self.r#for = Some(duration);
        self
    }
}

impl ZoneTrigger {
    pub fn new(entity_id: impl Into<String>, zone: impl Into<String>, event: ZoneEvent) -> Self {
        Self { entity_id: entity_id.into(), zone: zone.into(), event, id: None }
    }
}

impl EventTrigger {
    pub fn new(event_type: impl Into<String>) -> Self {
        Self::any_of([event_type.into()])
    }

    /// Fires on events of any of these types.
    pub fn any_of<I, S>(event_types: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self { event_type: strings(event_types), event_data: None, id: None }
    }

    /// Only events whose data contains these fields.
    ///
    /// Fails with [`Error::InvalidInput`](crate::Error::InvalidInput) if `data` does not
    /// serialize to a JSON object.
    pub fn event_data(mut self, data: impl Serialize) -> crate::Result<Self> {
        match serde_json::to_value(data) {
            Ok(Value::Object(map)) => {
                self.event_data = Some(map);
                Ok(self)
            }
            Ok(other) => Err(crate::Error::InvalidInput(format!("event_data must be a JSON object, got {other}"))),
            Err(e) => Err(crate::Error::InvalidInput(format!("event_data does not serialize: {e}"))),
        }
    }
}

impl HomeAssistantTrigger {
    pub fn new(event: HomeAssistantEvent) -> Self {
        Self { event, id: None }
    }
}

impl MqttTrigger {
    pub fn new(topic: impl Into<String>) -> Self {
        Self { topic: topic.into(), payload: None, value_template: None, encoding: None, qos: None, id: None }
    }

    pub fn payload(mut self, payload: impl Into<String>) -> Self {
        self.payload = Some(payload.into());
        self
    }

    /// Template applied to the message before it is compared with the payload
    /// (`value_json` gives access to a JSON message).
    pub fn value_template(mut self, template: impl Into<String>) -> Self {
        self.value_template = Some(template.into());
        self
    }

    pub fn encoding(mut self, encoding: impl Into<String>) -> Self {
        self.encoding = Some(encoding.into());
        self
    }

    /// Quality of service, 0 to 2.
    pub fn qos(mut self, qos: u8) -> Self {
        self.qos = Some(qos);
        self
    }
}

impl WebhookTrigger {
    pub fn new(webhook_id: impl Into<String>) -> Self {
        Self { webhook_id: webhook_id.into(), allowed_methods: Vec::new(), local_only: None, id: None }
    }

    /// Accepted HTTP methods (Home Assistant's default is `POST` and `PUT`).
    pub fn allowed_methods(mut self, methods: impl IntoIterator<Item = WebhookMethod>) -> Self {
        self.allowed_methods = methods.into_iter().collect();
        self
    }

    /// `false` accepts requests from outside the local network.
    pub fn local_only(mut self, local_only: bool) -> Self {
        self.local_only = Some(local_only);
        self
    }
}

impl CalendarTrigger {
    pub fn new(entity_id: impl Into<String>, event: CalendarEvent) -> Self {
        Self { entity_id: entity_id.into(), event, offset: None, id: None }
    }

    /// Negative values fire before the event start/end.
    pub fn offset(mut self, offset: TimeDelta) -> Self {
        self.offset = Some(offset);
        self
    }
}

impl TagTrigger {
    pub fn new(tag_id: impl Into<String>) -> Self {
        Self { tag_id: vec![tag_id.into()], device_id: Vec::new(), id: None }
    }

    /// Adds another tag.
    pub fn and_tag(mut self, tag_id: impl Into<String>) -> Self {
        self.tag_id.push(tag_id.into());
        self
    }

    /// Only scans by this device (adds to the list of accepted devices).
    pub fn device(mut self, device_id: impl Into<String>) -> Self {
        self.device_id.push(device_id.into());
        self
    }
}

impl ConversationTrigger {
    pub fn new(command: impl Into<String>) -> Self {
        Self { command: vec![command.into()], id: None }
    }

    /// Adds another sentence.
    pub fn and_command(mut self, command: impl Into<String>) -> Self {
        self.command.push(command.into());
        self
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

    fn wire(t: impl Into<Trigger>) -> Value {
        serde_json::to_value(t.into()).unwrap()
    }

    #[test]
    fn state_trigger_wire_format() {
        let t = StateTrigger::new("lock.front")
            .to(HaState::On)
            .from(HaState::Lock(LockState::Locked))
            .for_duration(Duration::from_secs(90));
        assert_eq!(
            wire(t),
            json!({"trigger": "state", "entity_id": ["lock.front"], "to": "on", "from": "locked", "for": "00:01:30"})
        );
    }

    #[test]
    fn state_trigger_matchers() {
        // Custom states keep their case (an `input_select` option `Home` is not `home`).
        assert_eq!(
            wire(StateTrigger::new("input_select.mode").to("Home").from("Open")),
            json!({"trigger": "state", "entity_id": ["input_select.mode"], "to": "Home", "from": "Open"})
        );
        assert_eq!(
            wire(StateTrigger::new("sensor.a").and_entity("sensor.b").id("s")),
            json!({"trigger": "state", "entity_id": ["sensor.a", "sensor.b"], "id": "s"})
        );
        assert_eq!(
            wire(StateTrigger::entities(["a.x", "a.y"]).to_any_of(["off", "unavailable"]).from_any_of([HaState::On])),
            json!({"trigger": "state", "entity_id": ["a.x", "a.y"], "to": ["off", "unavailable"], "from": ["on"]})
        );
        // `null` is sent, not omitted: any state change, but not attribute-only changes.
        let t = wire(StateTrigger::new("sensor.a").to_any_state().from_any_state());
        assert_eq!(t, json!({"trigger": "state", "entity_id": ["sensor.a"], "to": null, "from": null}));
        assert!(t.as_object().unwrap().contains_key("to"));
        assert_eq!(
            wire(StateTrigger::new("sensor.a").not_to(["unavailable", "unknown"]).not_from(["unknown"]).attribute("x")),
            json!({"trigger": "state", "entity_id": ["sensor.a"], "not_to": ["unavailable", "unknown"],
                   "not_from": ["unknown"], "attribute": "x"})
        );
    }

    #[test]
    fn numeric_state_wire_format() {
        assert_eq!(
            wire(NumericStateTrigger::new("sensor.t").above(20.5).attribute("temperature")),
            json!({"trigger": "numeric_state", "entity_id": ["sensor.t"], "above": 20.5, "attribute": "temperature"})
        );
        assert_eq!(
            wire(
                NumericStateTrigger::entities(["sensor.a", "sensor.b"])
                    .above_entity("input_number.max")
                    .below(30.0)
                    .value_template("{{ state.state | float }}")
                    .for_duration(Duration::from_secs(60))
            ),
            json!({"trigger": "numeric_state", "entity_id": ["sensor.a", "sensor.b"], "above": "input_number.max",
                   "below": 30.0, "value_template": "{{ state.state | float }}", "for": "00:01:00"})
        );
        assert_eq!(
            wire(NumericStateTrigger::new("sensor.t").and_entity("sensor.u").below_entity("sensor.limit")),
            json!({"trigger": "numeric_state", "entity_id": ["sensor.t", "sensor.u"], "below": "sensor.limit"})
        );
    }

    #[test]
    fn time_based_wire_format() {
        assert_eq!(wire(TimeTrigger::new("07:30")), json!({"trigger": "time", "at": ["07:30"]}));
        assert_eq!(
            wire(TimeTrigger::new("07:30:00").and_at("input_datetime.alarm").id("morning")),
            json!({"trigger": "time", "at": ["07:30:00", "input_datetime.alarm"], "id": "morning"})
        );
        assert_eq!(wire(TimePatternTrigger::new().minutes("/5")), json!({"trigger": "time_pattern", "minutes": "/5"}));
        assert_eq!(
            wire(TimePatternTrigger::new().hours("1").seconds("/30").id("p")),
            json!({"trigger": "time_pattern", "hours": "1", "seconds": "/30", "id": "p"})
        );
        assert_eq!(
            wire(SunTrigger::new(SunEvent::Sunset).offset(TimeDelta::minutes(-30)).id("dusk")),
            json!({"trigger": "sun", "event": "sunset", "offset": "-00:30:00", "id": "dusk"})
        );
        assert_eq!(wire(SunTrigger::new(SunEvent::Sunrise)), json!({"trigger": "sun", "event": "sunrise"}));
    }

    #[test]
    fn template_and_zone_wire_format() {
        assert_eq!(
            wire(TemplateTrigger::new("{{ true }}").id("t")),
            json!({"trigger": "template", "value_template": "{{ true }}", "id": "t"})
        );
        assert_eq!(
            wire(ZoneTrigger::new("person.a", "zone.home", ZoneEvent::Leave).id("z")),
            json!({"trigger": "zone", "entity_id": "person.a", "zone": "zone.home", "event": "leave", "id": "z"})
        );
    }

    #[test]
    fn event_wire_format() {
        assert_eq!(wire(EventTrigger::new("my_event")), json!({"trigger": "event", "event_type": ["my_event"]}));
        let t = EventTrigger::any_of(["a", "b"]).event_data(json!({"command": "play"})).unwrap().id("e");
        assert_eq!(
            wire(t),
            json!({"trigger": "event", "event_type": ["a", "b"], "event_data": {"command": "play"}, "id": "e"})
        );
        assert!(matches!(EventTrigger::new("a").event_data(json!([1])), Err(crate::Error::InvalidInput(_))));
    }

    #[test]
    fn homeassistant_wire_format() {
        assert_eq!(
            wire(HomeAssistantTrigger::new(HomeAssistantEvent::Shutdown)),
            json!({"trigger": "homeassistant", "event": "shutdown"})
        );
        assert_eq!(
            wire(HomeAssistantTrigger::new(HomeAssistantEvent::Start).id("boot")),
            json!({"trigger": "homeassistant", "event": "start", "id": "boot"})
        );
    }

    #[test]
    fn mqtt_wire_format() {
        assert_eq!(wire(MqttTrigger::new("a/b")), json!({"trigger": "mqtt", "topic": "a/b"}));
        assert_eq!(
            wire(MqttTrigger::new("a/b").payload("on").value_template("{{ value_json.state }}").encoding("utf-8").qos(1).id("m")),
            json!({"trigger": "mqtt", "topic": "a/b", "payload": "on", "value_template": "{{ value_json.state }}",
                   "encoding": "utf-8", "qos": 1, "id": "m"})
        );
    }

    #[test]
    fn webhook_wire_format() {
        assert_eq!(wire(WebhookTrigger::new("hook")), json!({"trigger": "webhook", "webhook_id": "hook"}));
        assert_eq!(
            wire(WebhookTrigger::new("hook").allowed_methods([WebhookMethod::Post, WebhookMethod::Get]).local_only(false).id("w")),
            json!({"trigger": "webhook", "webhook_id": "hook", "allowed_methods": ["POST", "GET"],
                   "local_only": false, "id": "w"})
        );
    }

    #[test]
    fn calendar_wire_format() {
        assert_eq!(
            wire(CalendarTrigger::new("calendar.work", CalendarEvent::Start)),
            json!({"trigger": "calendar", "entity_id": "calendar.work", "event": "start"})
        );
        assert_eq!(
            wire(CalendarTrigger::new("calendar.work", CalendarEvent::End).offset(TimeDelta::minutes(-15)).id("c")),
            json!({"trigger": "calendar", "entity_id": "calendar.work", "event": "end", "offset": "-00:15:00", "id": "c"})
        );
    }

    #[test]
    fn tag_and_conversation_wire_format() {
        assert_eq!(wire(TagTrigger::new("A7-6B")), json!({"trigger": "tag", "tag_id": ["A7-6B"]}));
        assert_eq!(
            wire(TagTrigger::new("A7-6B").and_tag("04-B1").device("dev1").id("t")),
            json!({"trigger": "tag", "tag_id": ["A7-6B", "04-B1"], "device_id": ["dev1"], "id": "t"})
        );
        assert_eq!(
            wire(ConversationTrigger::new("[it's ]party time")),
            json!({"trigger": "conversation", "command": ["[it's ]party time"]})
        );
        assert_eq!(
            wire(ConversationTrigger::new("a").and_command("b").id("c")),
            json!({"trigger": "conversation", "command": ["a", "b"], "id": "c"})
        );
    }

    #[test]
    fn durations() {
        let t = TemplateTrigger::new("{{ true }}").for_duration(Duration::from_millis(100_500_250));
        assert_eq!(wire(t)["for"], "27:55:00.250000");

        let sun = |offset: Option<TimeDelta>| {
            let t = SunTrigger::new(SunEvent::Sunset);
            wire(offset.map_or(t.clone(), |o| t.offset(o)))
        };
        assert_eq!(sun(Some(TimeDelta::minutes(-30)))["offset"], "-00:30:00");
        assert_eq!(sun(Some(TimeDelta::seconds(3661)))["offset"], "01:01:01");
        assert!(sun(None).get("offset").is_none());
    }
}
