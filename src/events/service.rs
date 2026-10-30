use serde::Deserialize;
use serde_json::Value;

use crate::events::Context;
use crate::state::{Domain, Service};

/// A service call on the event bus (`call_service` event), from
/// [`subscribe_service_calls`](crate::HAClient::subscribe_service_calls).
#[derive(Debug, Clone, Deserialize)]
pub struct ServiceCall {
    pub domain: Domain,
    pub service: Service,
    #[serde(default)]
    pub service_data: Value,
    /// The context of the call (who made it), taken from the bus event rather than its data.
    #[serde(skip)]
    pub context: Option<Context>,
}
