pub mod attributes;
pub mod helpers;
pub mod state;
pub mod service;

pub use attributes::{
    AttributeSet, ClimateAttributes, GenericAttributes, HistoricalAttributeSet,
    HistoricalClimateAttributes, HistoricalGenericAttributes,
};
pub use state::{HistoricalState, State};
pub use service::ServiceCalledEvent;

use serde_json::Value;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct TriggerFiredEvent {
    pub name: String,
    pub description: String,
}

/// Represents a deserialized, type-safe Home Assistant event.
/// This enum is passed to event callbacks.
#[derive(Debug, Clone)]
pub enum Event {
    /// Corresponds to the `state_changed` event type.
    /// Triggered when the state of a subscribed entity changes.
    StateChanged {
        entity_id: String,
        new_state: State,
        old_state: Option<State>,
    },

    /// Corresponds to the `call_service` event type.
    /// Triggered when a service is called in Home Assistant.
    ServiceCalled(ServiceCalledEvent),

    /// Corresponds to a trigger firing from a `subscribe_triggers` subscription.
    TriggerFired(TriggerFiredEvent),

    /// A generic fallback for any event that doesn't have a specific
    /// type-safe variant in this enum. It provides the raw `event_type`
    /// string and the JSON `data` payload.
    Generic {
        event_type: String,
        data: Value,
    },
}
