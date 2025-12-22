use serde::Deserialize;
use serde_json::Value;

/// Represents the detailed state of a Home Assistant entity.
/// This struct is used in events like `state_changed`.
#[derive(Deserialize, Debug, Clone)]
pub struct State {
    pub entity_id: String,
    pub state: String,
    #[serde(default)] // Attributes can be an empty map
    pub attributes: Value,
    pub last_changed: String,
    pub last_updated: String,
    pub context: Value,
}

/// A temporary struct to deserialize the `data` field of a `state_changed` event.
/// This is an internal implementation detail.
#[derive(Deserialize, Debug, Clone)]
pub(super) struct StateChangedData {
    pub entity_id: String,
    pub new_state: State,
    pub old_state: State,
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
        old_state: State,
    },

    /// A generic fallback for any event that doesn't have a specific
    /// type-safe variant in this enum. It provides the raw `event_type`
    /// string and the JSON `data` payload.
    Generic {
        event_type: String,
        data: Value,
    },
}
