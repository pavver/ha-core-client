//! [`Source`]: what a subscription listens to and which type it delivers.

use std::time::Duration;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::{Error, Result};
use crate::events::{
    BusEvent, ServiceCall, Silence, StateChange, SubEvent, TemplateUpdate, TriggerFired, Update,
};
use crate::protocol::{self, Request};
use crate::subscription::{EntityChanges, SubKind};
use crate::triggers::Trigger;

/// Describes one subscription: what to subscribe to and which type `T` it delivers.
///
/// Pass it to [`HAClient::subscribe`](crate::HAClient::subscribe) (callback) or
/// [`HAClient::stream`](crate::HAClient::stream) (a [`Stream`](futures_util::Stream)). The
/// convenience methods of `HAClient` (`subscribe_entity`, …) are shortcuts for the first.
///
/// ```no_run
/// # use ha_core_client::{HAClient, Source};
/// # async fn example(client: HAClient) -> ha_core_client::Result<()> {
/// let _stream = client.stream(Source::entity("sensor.temperature")).await?;
/// # Ok(())
/// # }
/// ```
pub struct Source<T> {
    pub(crate) kind: SubKind,
    pub(crate) request: Request,
    /// `None`: a data event that does not fit `T`; it is logged and skipped.
    pub(crate) convert: fn(SubEvent) -> Option<T>,
}

impl<T> std::fmt::Debug for Source<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Source").field("kind", &self.kind).field("request", &self.request).finish()
    }
}

/// Turns a queued event into what the user receives. A data event that does not fit the
/// source's type is logged and skipped (`None`); statuses always pass.
pub(crate) fn into_update<T>(convert: fn(SubEvent) -> Option<T>, event: SubEvent, sub_id: u64) -> Option<Update<T>> {
    match event {
        SubEvent::Status { subscription, status } => Some(Update::Status { subscription, status }),
        data => {
            let update = convert(data).map(Update::Event);
            if update.is_none() {
                log::warn!("Subscription {sub_id}: skipping an event that does not fit the subscription's type");
            }
            update
        }
    }
}

impl<T> Source<T> {
    /// Keeps every change of an entity subscription when the callback falls behind.
    ///
    /// By default the queue merges consecutive changes of the entity (the first
    /// `old_state` with the last `new_state`) and reports them as
    /// [`Lagged`](crate::SubscriptionStatus::Lagged). Without merging, the queue behaves
    /// like that of other subscriptions: past
    /// [`subscription_queue_size`](crate::ClientConfig::subscription_queue_size) it drops
    /// the oldest events, also reporting `Lagged`. With [`EntityChanges::State`],
    /// attribute-only changes are still dropped.
    ///
    /// Has no effect on sources that are not entity subscriptions.
    pub fn without_merging(mut self) -> Self {
        if let SubKind::EntityState { merge, .. } = &mut self.kind {
            *merge = false;
        }
        self
    }
}

impl Source<StateChange> {
    /// Changes of one entity (state and attributes). See
    /// [`subscribe_entity`](crate::HAClient::subscribe_entity).
    pub fn entity(entity_id: impl Into<String>) -> Self {
        Self::entity_with(entity_id, EntityChanges::Any)
    }

    /// Changes of one entity, choosing which updates arrive. See
    /// [`subscribe_entity_with`](crate::HAClient::subscribe_entity_with).
    pub fn entity_with(entity_id: impl Into<String>, changes: EntityChanges) -> Self {
        let entity_id = entity_id.into();
        // A `state` trigger rather than `subscribe_entities`: Home Assistant dispatches it by
        // entity id, while every `subscribe_entities` runs for all state changes. See ARCHITECTURE.md.
        let mut trigger = serde_json::json!({ "trigger": "state", "entity_id": entity_id });
        if changes == EntityChanges::State {
            // `to: null` makes Home Assistant skip attribute-only changes.
            trigger["to"] = Value::Null;
        }
        Source {
            request: protocol::subscribe_trigger(&trigger),
            kind: SubKind::EntityState { entity_id, changes, merge: true },
            convert: |event| match event {
                SubEvent::StateChanged(change) => Some(change),
                _ => None,
            },
        }
    }

    /// Every state change in the system (bus event `state_changed`). See
    /// [`subscribe_state_changes`](crate::HAClient::subscribe_state_changes).
    pub fn state_changes() -> Self {
        Source {
            request: protocol::subscribe_events("state_changed"),
            kind: SubKind::StateChangedEvents,
            convert: |event| match event {
                SubEvent::StateChanged(change) => Some(change),
                _ => None,
            },
        }
    }
}

impl Source<ServiceCall> {
    /// Every `call_service` event on the bus.
    pub fn service_calls() -> Self {
        Source {
            request: protocol::subscribe_events("call_service"),
            kind: SubKind::ServiceCallEvents,
            convert: |event| match event {
                SubEvent::ServiceCall(call) => Some(call),
                _ => None,
            },
        }
    }
}

impl Source<BusEvent> {
    /// Bus events of one type, as they are (`state_changed` and `call_service` are not
    /// special-cased; use [`state_changes`](Source::state_changes) and
    /// [`service_calls`](Source::service_calls) for typed ones).
    pub fn events(event_type: &str) -> Self {
        Source {
            request: protocol::subscribe_events(event_type),
            kind: SubKind::Events,
            convert: |event| match event {
                SubEvent::Bus(bus) => Some(bus),
                _ => None,
            },
        }
    }
}

impl Source<TriggerFired> {
    /// A Home Assistant trigger: a [`Trigger`] or any of its platform structs.
    pub fn trigger(trigger: impl Into<Trigger>) -> Self {
        let trigger = serde_json::to_value(trigger.into()).expect("a Trigger always serializes to a JSON object");
        Self::trigger_object(trigger)
    }

    /// A trigger given as raw JSON, for platforms and options not modelled by [`Trigger`].
    ///
    /// Fails with [`Error::InvalidInput`] if `trigger` is not a JSON object.
    pub fn trigger_raw(trigger: Value) -> Result<Self> {
        if !trigger.is_object() {
            return Err(Error::InvalidInput(format!("a trigger must be a JSON object, got {trigger}")));
        }
        Ok(Self::trigger_object(trigger))
    }

    fn trigger_object(trigger: Value) -> Self {
        Source {
            request: protocol::subscribe_trigger(&trigger),
            kind: SubKind::Trigger,
            convert: |event| match event {
                SubEvent::TriggerFired(fired) => Some(fired),
                _ => None,
            },
        }
    }
}

impl Source<TemplateUpdate<Value>> {
    /// A template rendered by Home Assistant; results arrive as JSON values.
    pub fn template(template: &str) -> Self {
        Self::template_as::<Value>(template)
    }

    /// Like [`template`](Self::template), decoding each result into `T`; a result that does
    /// not fit arrives as [`TemplateUpdate::DecodeError`].
    pub fn template_as<T: DeserializeOwned + Send + 'static>(template: &str) -> Source<TemplateUpdate<T>> {
        Source {
            request: protocol::render_template(template),
            kind: SubKind::Template,
            convert: convert_template::<T>,
        }
    }
}

fn convert_template<T: DeserializeOwned>(event: SubEvent) -> Option<TemplateUpdate<T>> {
    match event {
        SubEvent::TemplateRendered { result } => Some(match serde_json::from_value::<T>(result.clone()) {
            Ok(value) => TemplateUpdate::Rendered(value),
            Err(e) => TemplateUpdate::DecodeError { result, error: e.to_string() },
        }),
        SubEvent::TemplateError { message, level } => Some(TemplateUpdate::RenderError { message, level }),
        _ => None,
    }
}

impl Source<Silence> {
    /// Whether an entity keeps reporting. See [`watch_silence`](crate::HAClient::watch_silence).
    ///
    /// Fails with [`Error::InvalidInput`] for a malformed entity id.
    pub fn silence(entity_id: &str, max_silence: Duration) -> Result<Self> {
        let template = silence_template(entity_id, max_silence)?;
        Ok(Source {
            request: protocol::render_template(&template),
            kind: SubKind::Silence { entity_id: entity_id.to_string() },
            convert: |event| match event {
                SubEvent::Silence(silence) => Some(silence),
                _ => None,
            },
        })
    }
}

/// The template of [`HAClient::watch_silence`](crate::HAClient::watch_silence): renders `true`
/// while the entity is missing or its last report is older than `max_silence`. The id is
/// checked because it is part of the template. The object id is subscripted, since
/// `states.sensor.1st_floor` is a Jinja syntax error.
fn silence_template(entity_id: &str, max_silence: Duration) -> Result<String> {
    let part_ok = |p: &str| !p.is_empty() && p.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    match entity_id.split_once('.') {
        Some((domain, object_id)) if part_ok(domain) && part_ok(object_id) => Ok(format!(
            "{{% set s = states.{domain}['{object_id}'] %}}{{{{ s is none or (now() - s.last_reported).total_seconds() > {} }}}}",
            max_silence.as_secs_f64()
        )),
        _ => Err(Error::InvalidInput(format!("invalid entity_id: {entity_id}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn silence_template_checks_the_entity_id() {
        assert_eq!(
            silence_template("sensor.outdoor_2", Duration::from_secs(1800)).unwrap(),
            "{% set s = states.sensor['outdoor_2'] %}{{ s is none or (now() - s.last_reported).total_seconds() > 1800 }}"
        );
        // Object ids may start with a digit.
        assert!(silence_template("sensor.1st_floor", Duration::from_secs(1)).unwrap().contains("states.sensor['1st_floor']"));
        for bad in ["sensor", "sensor.", ".x", "sensor.x }}{{ 1", "Sensor.x", "sensor.x.y"] {
            assert!(matches!(silence_template(bad, Duration::from_secs(1)), Err(Error::InvalidInput(_))), "{bad}");
        }
    }

    #[test]
    fn template_results_are_decoded_or_reported() {
        let rendered = |result: Value| SubEvent::TemplateRendered { result };
        assert!(matches!(convert_template::<f64>(rendered(json!(2.5))), Some(TemplateUpdate::Rendered(v)) if v == 2.5));
        assert!(matches!(
            convert_template::<f64>(rendered(json!("x"))),
            Some(TemplateUpdate::DecodeError { ref result, .. }) if result == "x"
        ));
        assert!(matches!(convert_template::<Option<f64>>(rendered(Value::Null)), Some(TemplateUpdate::Rendered(None))));
    }

    #[test]
    fn raw_trigger_must_be_an_object() {
        assert!(matches!(Source::trigger_raw(json!(["mqtt"])), Err(Error::InvalidInput(_))));
        assert!(Source::trigger_raw(json!({"trigger": "mqtt", "topic": "t"})).is_ok());
    }

    #[test]
    fn entity_source_requests() {
        let any = Source::entity("sensor.a");
        assert_eq!(any.request["trigger"], json!({"trigger": "state", "entity_id": "sensor.a"}));
        let state = Source::entity_with("sensor.a", EntityChanges::State);
        assert_eq!(state.request["trigger"]["to"], Value::Null);
        assert_eq!(Source::state_changes().request, json!({"type": "subscribe_events", "event_type": "state_changed"}));
        assert_eq!(Source::service_calls().request["event_type"], "call_service");
    }
}
