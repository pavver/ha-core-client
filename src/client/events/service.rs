use serde::Deserialize;
use serde_json::Value;

/// Represents the data payload for a `call_service` event.
#[derive(Debug, Clone, Deserialize)]
pub struct ServiceCalledEvent {
    pub domain: String,
    pub service: String,
    pub service_data: Value,
}
