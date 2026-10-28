pub mod attributes;
pub(crate) mod helpers;
pub mod state;
pub mod service;

pub use attributes::{AttributeSet, ClimateAttributes, GenericAttributes};
pub use state::{HistoricalState, State};
pub use service::ServiceCalledEvent;

use serde_json::Value;

use self::state::StateChangedData;
use crate::subscription::Subscription;

/// A trigger firing from a `subscribe_trigger` subscription.
///
/// Home Assistant sends `{"variables": {"trigger": {...}}, "context": ...}`; the
/// content of `trigger` depends on the platform (`state` triggers carry
/// `from_state`/`to_state`, `time_pattern` carries `now`, …).
#[derive(Debug, Clone)]
pub struct TriggerFiredEvent {
    /// The `variables.trigger` object.
    pub trigger: Value,
    /// The context of the change that fired the trigger, if any.
    pub context: Option<Value>,
}

impl TriggerFiredEvent {
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
        let context = payload.get_mut("context").map(Value::take).filter(|c| !c.is_null());
        Self { trigger, context }
    }
}

/// Represents a deserialized, type-safe Home Assistant event.
/// This enum is passed to event callbacks.
// States are kept unboxed so callbacks can pattern-match them directly.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Event {
    /// A state change of an entity (from `subscribe_entity` or a `state_changed` subscription).
    /// `old_state` is `None` when the entity was added, `new_state` when it was removed.
    StateChanged {
        entity_id: String,
        old_state: Option<State>,
        new_state: Option<State>,
    },

    /// Corresponds to the `call_service` event type.
    /// Triggered when a service is called in Home Assistant.
    ServiceCalled(ServiceCalledEvent),

    /// Corresponds to a trigger firing from a `subscribe_trigger` subscription.
    TriggerFired(TriggerFiredEvent),

    /// The current result of a [`subscribe_template`](crate::HAClient::subscribe_template)
    /// template: right after subscribing (and after every reconnect), then each time it changes.
    TemplateRendered { result: Value },

    /// Home Assistant failed to render a subscribed template. The subscription stays active.
    /// `level` is `ERROR` or `WARNING`.
    TemplateError { message: String, level: String },

    /// From [`watch_silence`](crate::HAClient::watch_silence): whether the entity has not
    /// reported for longer than the given time. Right after subscribing (and after every
    /// reconnect), then each time it changes.
    Silence { entity_id: String, silent: bool },

    /// A generic fallback for any event that doesn't have a specific
    /// type-safe variant in this enum. It provides the raw `event_type`
    /// string and the JSON `data` payload.
    Generic {
        event_type: String,
        data: Value,
    },

    /// A change in the subscription itself (connection lost, restored, …), delivered
    /// in order with the data events. `subscription` can be passed to
    /// [`HAClient::unsubscribe`](crate::HAClient::unsubscribe), also while disconnected.
    Status {
        subscription: Subscription,
        status: SubscriptionStatus,
    },
}

/// Lifecycle of a subscription, delivered as [`Event::Status`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubscriptionStatus {
    /// The connection was lost. No events arrive until [`Restored`](Self::Restored), so data
    /// derived from earlier events should be treated as unknown.
    Interrupted,
    /// Active again after a reconnect. For `subscribe_entity`, if the entity changed while
    /// disconnected, one `StateChanged` (last known state → current state) is delivered
    /// right before this.
    Restored,
    /// The callback fell behind and `dropped` events were lost before the events that
    /// follow. Entity subscriptions merge state changes instead of dropping them (the next
    /// `StateChanged` spans from the last delivered state to the newest one), so for them
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

impl Event {
    /// Converts a bus event (`{"event_type": ..., "data": ...}`) from `subscribe_events`.
    pub(crate) fn from_bus_event(mut payload: Value) -> Event {
        let event_type = payload
            .get("event_type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let data = payload.get_mut("data").map(Value::take).unwrap_or(Value::Null);
        let parsed = match event_type.as_str() {
            "state_changed" => serde_json::from_value::<StateChangedData>(data.clone()).map(Event::state_changed),
            "call_service" => serde_json::from_value(data.clone()).map(Event::ServiceCalled),
            _ => return Event::Generic { event_type, data },
        };
        parsed.unwrap_or_else(|e| {
            log::warn!("Failed to deserialize {event_type} event data: {e}. Falling back to Generic.");
            Event::Generic { event_type, data }
        })
    }

    /// Converts the payload of a `state` trigger used by `subscribe_entity`.
    pub(crate) fn from_entity_trigger(payload: Value) -> Event {
        let fired = TriggerFiredEvent::from_payload(payload);
        match serde_json::from_value::<StateChangedData>(fired.trigger.clone()) {
            Ok(d) => Event::state_changed(d),
            Err(e) => {
                log::warn!("Failed to deserialize entity state change: {e}. Falling back to TriggerFired.");
                Event::TriggerFired(fired)
            }
        }
    }

    /// Converts a `render_template` event: `{"result": ..., "listeners": ...}` or
    /// `{"error": ..., "level": ...}`.
    pub(crate) fn from_template(mut payload: Value) -> Event {
        if let Some(message) = payload.get("error") {
            let message = message.as_str().map_or_else(|| message.to_string(), str::to_string);
            let level = payload.get("level").and_then(Value::as_str).unwrap_or("ERROR").to_string();
            return Event::TemplateError { message, level };
        }
        Event::TemplateRendered { result: payload.get_mut("result").map(Value::take).unwrap_or(Value::Null) }
    }

    /// Converts the `render_template` event of `watch_silence`, whose template renders a boolean.
    pub(crate) fn from_silence(entity_id: &str, payload: Value) -> Event {
        match Event::from_template(payload) {
            Event::TemplateRendered { result } => match result.as_bool() {
                Some(silent) => Event::Silence { entity_id: entity_id.to_string(), silent },
                None => {
                    log::warn!("Unexpected silence template result for {entity_id}: {result}");
                    Event::TemplateRendered { result }
                }
            },
            other => other,
        }
    }

    fn state_changed(d: StateChangedData) -> Event {
        Event::StateChanged {
            entity_id: d.entity_id,
            old_state: d.old_state,
            new_state: d.new_state,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        match Event::from_entity_trigger(payload) {
            Event::StateChanged { entity_id, old_state, new_state } => {
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
        let fired = TriggerFiredEvent::from_payload(json!({
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
        let e = Event::from_bus_event(json!({
            "event_type": "state_changed",
            "data": {"entity_id": "sensor.solar", "old_state": state_json("1"), "new_state": state_json("2")}
        }));
        assert!(matches!(e, Event::StateChanged { new_state: Some(_), .. }));

        let e = Event::from_bus_event(json!({
            "event_type": "call_service",
            "data": {"domain": "light", "service": "turn_on", "service_data": {"entity_id": "light.a"}}
        }));
        assert!(matches!(e, Event::ServiceCalled(ref s) if s.service == crate::state::Service::TurnOn));

        let e = Event::from_bus_event(json!({"event_type": "my_event", "data": {"x": 1}}));
        assert!(matches!(e, Event::Generic { ref event_type, ref data } if event_type == "my_event" && data["x"] == 1));
    }
}
