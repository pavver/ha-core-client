use serde::Deserialize;
use serde_json::Value;

use crate::state::{Domain, Service};

/// Represents the data payload for a `call_service` event.
#[derive(Debug, Clone, Deserialize)]
pub struct ServiceCalledEvent {
    pub domain: Domain,
    pub service: Service,
    #[serde(default)]
    pub service_data: Value,
}
