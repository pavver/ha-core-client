use serde::Deserialize;
use serde_json::Value;

use crate::state::{Domain, Service};

/// A service call on the event bus (`call_service` event), from
/// [`subscribe_service_calls`](crate::HAClient::subscribe_service_calls).
#[derive(Debug, Clone, Deserialize)]
pub struct ServiceCall {
    pub domain: Domain,
    pub service: Service,
    #[serde(default)]
    pub service_data: Value,
}
