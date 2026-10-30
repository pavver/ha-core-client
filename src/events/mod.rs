pub mod attributes;
pub mod context;
pub(crate) mod helpers;
pub mod state;
pub mod service;

pub use attributes::*;
pub use context::Context;
pub use state::{HistoricalState, State};
pub use service::ServiceCall;
pub use state::StateChange;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::Result;

use crate::subscription::Subscription;

fn parse_context(value: Value) -> Context {
    serde_json::from_value(value).unwrap_or_default()
}

/// A trigger firing from a `subscribe_trigger` subscription (also the item of `Source::trigger`).
///
/// Home Assistant sends `{"variables": {"trigger": {...}}, "context": ...}`; the
/// content of `trigger` depends on the platform (`state` triggers carry
/// `from_state`/`to_state`, `time_pattern` carries `now`, …).
#[derive(Debug, Clone)]
pub struct TriggerFired {
    /// The `variables.trigger` object.
    pub trigger: Value,
    /// The context of the change that fired the trigger, if any.
    pub context: Option<Context>,
}

impl TriggerFired {
    /// The trigger platform (`state`, `time_pattern`, `sun`, …).
    pub fn platform(&self) -> Option<&str> {
        self.trigger.get("platform").and_then(Value::as_str)
    }

    /// Human-readable description Home Assistant generated for the trigger.
    pub fn description(&self) -> Option<&str> {
        self.trigger.get("description").and_then(Value::as_str)
    }

    /// The entity that fired a `state`/`numeric_state`/`zone` trigger.
    pub fn entity_id(&self) -> Option<&str> {
        self.trigger.get("entity_id").and_then(Value::as_str)
    }

    /// `from_state` of a state-based trigger.
    pub fn from_state(&self) -> Option<State> {
        self.state_field("from_state")
    }

    /// `to_state` of a state-based trigger.
    pub fn to_state(&self) -> Option<State> {
        self.state_field("to_state")
    }

    fn state_field(&self, key: &str) -> Option<State> {
        let v = self.trigger.get(key)?;
        if v.is_null() {
            return None;
        }
        serde_json::from_value(v.clone())
            .inspect_err(|e| log::warn!("Failed to parse trigger {key}: {e}"))
            .ok()
    }

    pub(crate) fn from_payload(payload: Value) -> Self {
        let mut payload = payload;
        let trigger = payload
            .get_mut("variables")
            .and_then(|v| v.get_mut("trigger"))
            .map(Value::take)
            .unwrap_or(Value::Null);
        let context = payload.get_mut("context").map(Value::take).filter(|c| !c.is_null()).map(parse_context);
        Self { trigger, context }
    }
}

/// What a subscription delivers: either one of its data events or a change of the
/// subscription itself, in order with the data.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Update<T> {
    /// A data event of the subscription.
    Event(T),
    /// A change of the subscription itself (connection lost, restored, lagged, …), in order with the data.
    /// `subscription` can be passed to [`HAClient::unsubscribe`](crate::HAClient::unsubscribe), also while disconnected.
    Status { subscription: Subscription, status: SubscriptionStatus },
}

impl<T> Update<T> {
    /// The data event, if this is one.
    pub fn into_event(self) -> Option<T> {
        match self {
            Update::Event(event) => Some(event),
            Update::Status { .. } => None,
        }
    }

    /// The data event, if this is one.
    pub fn event(&self) -> Option<&T> {
        match self {
            Update::Event(event) => Some(event),
            Update::Status { .. } => None,
        }
    }

    /// The status, if this is a status update.
    pub fn status(&self) -> Option<&SubscriptionStatus> {
        match self {
            Update::Event(_) => None,
            Update::Status { status, .. } => Some(status),
        }
    }
}

/// An event of the Home Assistant event bus, from [`subscribe_events`](crate::HAClient::subscribe_events).
#[derive(Debug, Clone)]
pub struct BusEvent {
    pub event_type: String,
    /// The event's `data` object (`Null` if absent).
    pub data: Value,
    /// `LOCAL` or `REMOTE`.
    pub origin: Option<String>,
    /// When the event was fired; `None` if absent or unparsable.
    pub time_fired: Option<DateTime<Utc>>,
    pub context: Option<Context>,
}

impl BusEvent {
    /// Decodes `data` into `T`. Fails with [`Error::Decode`].
    pub fn data_as<T: DeserializeOwned>(&self) -> Result<T> {
        Ok(serde_json::from_value(self.data.clone())?)
    }

    /// Converts a `subscribe_events` payload (`{"event_type": ..., "data": ..., ...}`).
    pub(crate) fn from_payload(mut payload: Value) -> Self {
        let event_type = payload.get("event_type").and_then(Value::as_str).unwrap_or_default().to_string();
        let data = payload.get_mut("data").map(Value::take).unwrap_or(Value::Null);
        let origin = payload.get("origin").and_then(Value::as_str).map(str::to_string);
        let time_fired = payload
            .get("time_fired")
            .and_then(Value::as_str)
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|t| t.with_timezone(&Utc));
        let context = payload.get_mut("context").map(Value::take).filter(|c| !c.is_null()).map(parse_context);
        Self { event_type, data, origin, time_fired, context }
    }
}

crate::state::string_enum! {
    /// The `level` of a failed template render.
    pub enum TemplateErrorLevel {
        Error => "ERROR",
        Warning => "WARNING",
    }
}

/// An update of a template subscription ([`subscribe_template`](crate::HAClient::subscribe_template),
/// [`subscribe_template_as`](crate::HAClient::subscribe_template_as)).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TemplateUpdate<T = Value> {
    /// The current result, decoded into `T`.
    Rendered(T),
    /// Home Assistant failed to render the template; the subscription stays active.
    RenderError { message: String, level: TemplateErrorLevel },
    /// The template rendered, but the result does not decode into `T`.
    DecodeError { result: Value, error: String },
}

/// From [`watch_silence`](crate::HAClient::watch_silence): whether the entity has not
/// reported for longer than the given time. Right after subscribing (and after every
/// reconnect), then each time it changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Silence {
    pub entity_id: String,
    pub silent: bool,
}

/// The event type the runner and the queue work with. Public types are derived from it by
/// each [`Source`](crate::Source)'s conversion, in the subscription's worker.
// States are kept unboxed so callbacks can pattern-match them directly.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub(crate) enum SubEvent {
    StateChanged(StateChange),
    ServiceCall(ServiceCall),
    Bus(BusEvent),
    TriggerFired(TriggerFired),
    /// A template result (not yet decoded into the subscription's type).
    TemplateRendered { result: Value },
    TemplateError { message: String, level: TemplateErrorLevel },
    Silence(Silence),
    Status { subscription: Subscription, status: SubscriptionStatus },
}

/// Lifecycle of a subscription, delivered as [`Update::Status`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubscriptionStatus {
    /// The connection was lost. No events arrive until [`Restored`](Self::Restored), so data
    /// derived from earlier events should be treated as unknown.
    Interrupted,
    /// Active again after a reconnect. For `subscribe_entity`, if the entity changed while
    /// disconnected, one `StateChange` (last known state → current state) is delivered
    /// right before this.
    Restored,
    /// The callback fell behind and `dropped` events were lost before the events that
    /// follow. Entity subscriptions merge state changes instead of dropping them (the next
    /// `StateChange` spans from the last delivered state to the newest one), so for them
    /// `dropped` counts merged changes. Other subscriptions drop their oldest events.
    /// See [`ClientConfig::subscription_queue_size`](crate::ClientConfig::subscription_queue_size).
    Lagged { dropped: u64 },
    /// Home Assistant rejected the subscription when it was restored after a reconnect.
    /// It has been removed; no further events.
    Rejected { code: crate::HaErrorCode, message: String },
    /// The client stopped (`stop()`, all handles dropped, or the token was rejected).
    /// No further events.
    Closed,
    /// The subscription was cancelled with [`HAClient::unsubscribe`](crate::HAClient::unsubscribe)
    /// (or by dropping its [`SubscriptionGuard`](crate::SubscriptionGuard)). No further events.
    Unsubscribed,
}

impl SubEvent {
    /// Converts a bus event from `subscribe_events`.
    pub(crate) fn from_bus_event(payload: Value) -> SubEvent {
        SubEvent::Bus(BusEvent::from_payload(payload))
    }

    /// Converts a `state_changed` bus event (`subscribe_state_changes`). Data that does not
    /// parse stays a plain `Bus` event, which the source's conversion skips.
    pub(crate) fn from_state_changed(payload: Value) -> SubEvent {
        let bus = BusEvent::from_payload(payload);
        match serde_json::from_value::<StateChange>(bus.data.clone()) {
            Ok(change) => SubEvent::StateChanged(change),
            Err(e) => {
                log::warn!("Failed to deserialize state_changed event data: {e}");
                SubEvent::Bus(bus)
            }
        }
    }

    /// Converts a `call_service` bus event (`subscribe_service_calls`).
    pub(crate) fn from_service_call(payload: Value) -> SubEvent {
        let bus = BusEvent::from_payload(payload);
        match serde_json::from_value::<ServiceCall>(bus.data.clone()) {
            Ok(mut call) => {
                call.context = bus.context;
                SubEvent::ServiceCall(call)
            }
            Err(e) => {
                log::warn!("Failed to deserialize call_service event data: {e}");
                SubEvent::Bus(bus)
            }
        }
    }

    /// Converts the payload of a `state` trigger used by `subscribe_entity`.
    pub(crate) fn from_entity_trigger(payload: Value) -> SubEvent {
        let fired = TriggerFired::from_payload(payload);
        match serde_json::from_value::<StateChange>(fired.trigger.clone()) {
            Ok(change) => SubEvent::StateChanged(change),
            Err(e) => {
                log::warn!("Failed to deserialize entity state change: {e}");
                SubEvent::TriggerFired(fired)
            }
        }
    }

    /// Converts a `render_template` event: `{"result": ..., "listeners": ...}` or
    /// `{"error": ..., "level": ...}`.
    pub(crate) fn from_template(mut payload: Value) -> SubEvent {
        if let Some(message) = payload.get("error") {
            let message = message.as_str().map_or_else(|| message.to_string(), str::to_string);
            let level = payload.get("level").and_then(Value::as_str).unwrap_or("ERROR").into();
            return SubEvent::TemplateError { message, level };
        }
        SubEvent::TemplateRendered { result: payload.get_mut("result").map(Value::take).unwrap_or(Value::Null) }
    }

    /// Converts the `render_template` event of `watch_silence`, whose template renders a boolean.
    pub(crate) fn from_silence(entity_id: &str, payload: Value) -> SubEvent {
        match SubEvent::from_template(payload) {
            SubEvent::TemplateRendered { result } => match result.as_bool() {
                Some(silent) => SubEvent::Silence(Silence { entity_id: entity_id.to_string(), silent }),
                None => {
                    log::warn!("Unexpected silence template result for {entity_id}: {result}");
                    SubEvent::TemplateRendered { result }
                }
            },
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use serde_json::json;

    fn state_json(state: &str) -> Value {
        json!({
            "entity_id": "sensor.solar", "state": state,
            "attributes": {"unit_of_measurement": "W", "device_class": "power", "state_class": "measurement"},
            "last_changed": "2026-10-28T14:45:09.185007+00:00",
            "last_reported": "2026-10-28T14:45:09.185007+00:00",
            "last_updated": "2026-10-28T14:45:09.185007+00:00",
            "context": {"id": "01X", "parent_id": null, "user_id": null}
        })
    }

    // Shape captured from a live `subscribe_trigger` (platform: state) event.
    #[test]
    fn entity_trigger_becomes_state_changed() {
        let payload = json!({
            "variables": {"trigger": {
                "id": "0", "idx": "0", "alias": null, "platform": "state",
                "entity_id": "sensor.solar",
                "from_state": state_json("31"), "to_state": state_json("19"),
                "for": null, "attribute": null, "description": "state of sensor.solar"
            }},
            "context": {"id": "01X", "parent_id": null, "user_id": null}
        });
        match SubEvent::from_entity_trigger(payload) {
            SubEvent::StateChanged(StateChange { entity_id, old_state, new_state }) => {
                assert_eq!(entity_id, "sensor.solar");
                assert_eq!(old_state.unwrap().state, "31");
                assert_eq!(new_state.unwrap().state, "19");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    // Shape captured from a live `subscribe_trigger` (platform: time_pattern) event.
    #[test]
    fn trigger_fired_payload() {
        let fired = TriggerFired::from_payload(json!({
            "variables": {"trigger": {"id": "0", "idx": "0", "alias": null, "platform": "time_pattern",
                "now": "2026-10-28T16:39:05.290879+02:00", "description": "time pattern"}},
            "context": null
        }));
        assert_eq!(fired.platform(), Some("time_pattern"));
        assert_eq!(fired.description(), Some("time pattern"));
        assert!(fired.context.is_none());
        assert!(fired.to_state().is_none());
    }

    #[test]
    fn bus_events() {
        let e = SubEvent::from_state_changed(json!({
            "event_type": "state_changed",
            "data": {"entity_id": "sensor.solar", "old_state": state_json("1"), "new_state": state_json("2")}
        }));
        assert!(matches!(e, SubEvent::StateChanged(StateChange { new_state: Some(_), .. })));

        let e = SubEvent::from_service_call(json!({
            "event_type": "call_service",
            "data": {"domain": "light", "service": "turn_on", "service_data": {"entity_id": "light.a"}}
        }));
        assert!(matches!(e, SubEvent::ServiceCall(ref s) if s.service == crate::state::Service::TurnOn));

        // The context sits on the event, not in its data.
        let e = SubEvent::from_service_call(json!({
            "event_type": "call_service",
            "data": {"domain": "light", "service": "turn_on", "service_data": {}},
            "context": {"id": "01Y", "parent_id": null, "user_id": "abc"}
        }));
        match e {
            SubEvent::ServiceCall(s) => assert_eq!(s.context.unwrap().user_id.as_deref(), Some("abc")),
            other => panic!("unexpected {other:?}"),
        }

        // Data of the wrong shape stays a plain bus event.
        let e = SubEvent::from_service_call(json!({"event_type": "call_service", "data": {"x": 1}}));
        assert!(matches!(e, SubEvent::Bus(_)));

        // `subscribe_events` never special-cases the type.
        let e = SubEvent::from_bus_event(json!({"event_type": "state_changed", "data": {"x": 1}}));
        assert!(matches!(e, SubEvent::Bus(ref b) if b.event_type == "state_changed" && b.data["x"] == 1));
    }

    // Shape captured from a live `subscribe_events` event.
    fn bus_payload() -> Value {
        json!({
            "event_type": "my_event", "data": {"x": 1, "name": "a"}, "origin": "LOCAL",
            "time_fired": "2026-10-28T14:45:09.185007+00:00",
            "context": {"id": "01X", "parent_id": null, "user_id": null}
        })
    }

    #[test]
    fn bus_event_context_is_lenient() {
        let mut p = bus_payload();
        p["context"] = json!(42);
        assert_eq!(BusEvent::from_payload(p).context, Some(Context::default()));
        let mut p = bus_payload();
        p["context"] = json!("01ABC");
        assert_eq!(BusEvent::from_payload(p).context.unwrap().id, "01ABC");
    }

    #[test]
    fn bus_event_fields_and_data_as() {
        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct MyData {
            x: u32,
            name: String,
        }
        let e = BusEvent::from_payload(bus_payload());
        assert_eq!(e.event_type, "my_event");
        assert_eq!(e.origin.as_deref(), Some("LOCAL"));
        assert_eq!(e.time_fired.unwrap().timestamp_micros(), 1793198709185007);
        assert_eq!(e.context.as_ref().unwrap().id, "01X");
        assert_eq!(e.data_as::<MyData>().unwrap(), MyData { x: 1, name: "a".into() });
        assert!(matches!(e.data_as::<Vec<u32>>(), Err(Error::Decode(_))));
    }

    #[test]
    fn bus_event_tolerates_missing_and_bad_fields() {
        let e = BusEvent::from_payload(json!({"event_type": "e", "time_fired": "yesterday", "context": null}));
        assert_eq!(e.data, Value::Null);
        assert!(e.origin.is_none() && e.time_fired.is_none() && e.context.is_none());
    }

    #[test]
    fn template_events() {
        let e = SubEvent::from_template(json!({"error": "ZeroDivisionError", "level": "WARNING"}));
        assert!(matches!(e, SubEvent::TemplateError { level: TemplateErrorLevel::Warning, .. }));
        let e = SubEvent::from_template(json!({"result": 2, "listeners": {}}));
        assert!(matches!(e, SubEvent::TemplateRendered { ref result } if *result == 2));
    }

    #[test]
    fn update_accessors() {
        let event = Update::Event(5);
        assert_eq!(event.event(), Some(&5));
        assert!(event.status().is_none());
        assert_eq!(event.into_event(), Some(5));
        let status: Update<u8> =
            Update::Status { subscription: Subscription { id: 1, client: 1 }, status: SubscriptionStatus::Closed };
        assert_eq!(status.status(), Some(&SubscriptionStatus::Closed));
        assert!(status.event().is_none() && status.into_event().is_none());
    }
}
