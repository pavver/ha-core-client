//! Wire format of the Home Assistant WebSocket API.

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::Error;

/// A command body without its `id`; the runner assigns ids per connection.
pub(crate) type Request = Value;

pub(crate) fn auth(token: &str) -> String {
    json!({ "type": "auth", "access_token": token }).to_string()
}

pub(crate) fn with_id(id: u64, mut request: Request) -> String {
    request["id"] = id.into();
    request.to_string()
}

pub(crate) fn ping() -> Request {
    json!({ "type": "ping" })
}

pub(crate) fn subscribe_events(event_type: &str) -> Request {
    json!({ "type": "subscribe_events", "event_type": event_type })
}

pub(crate) fn subscribe_trigger(trigger: &Value) -> Request {
    json!({ "type": "subscribe_trigger", "trigger": trigger })
}

pub(crate) fn subscribe_entities(entity_ids: &[&str]) -> Request {
    json!({ "type": "subscribe_entities", "entity_ids": entity_ids })
}

pub(crate) fn unsubscribe(subscription: u64) -> Request {
    json!({ "type": "unsubscribe_events", "subscription": subscription })
}

pub(crate) fn get_states() -> Request {
    json!({ "type": "get_states" })
}

pub(crate) fn call_service(
    domain: &str,
    service: &str,
    entity_id: Option<&str>,
    service_data: Option<Value>,
    return_response: bool,
) -> Request {
    let mut msg = json!({ "type": "call_service", "domain": domain, "service": service });
    if let Some(data) = service_data {
        msg["service_data"] = data;
    }
    if let Some(entity_id) = entity_id {
        msg["target"] = json!({ "entity_id": entity_id });
    }
    if return_response {
        msg["return_response"] = true.into();
    }
    msg
}

pub(crate) fn history_during_period(
    entity_id: &str,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    significant_changes_only: bool,
) -> Request {
    json!({
        "type": "history/history_during_period",
        "entity_ids": [entity_id],
        "start_time": start_time,
        "end_time": end_time,
        "significant_changes_only": significant_changes_only,
    })
}

/// Any message sent by Home Assistant.
#[derive(Debug, Deserialize)]
pub(crate) struct Incoming {
    #[serde(rename = "type")]
    pub msg_type: String,
    #[serde(default)]
    pub id: Option<u64>,
    #[serde(default)]
    pub success: Option<bool>,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub error: Option<HaError>,
    #[serde(default)]
    pub event: Option<Value>,
    /// Set on `auth_invalid`.
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct HaError {
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub message: String,
}

impl From<HaError> for Error {
    fn from(e: HaError) -> Self {
        Error::Ha { code: e.code, message: e.message }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_service_uses_target() {
        let v = call_service("light", "turn_on", Some("light.a"), Some(json!({"brightness": 10})), true);
        assert_eq!(
            v,
            json!({"type": "call_service", "domain": "light", "service": "turn_on",
                   "service_data": {"brightness": 10}, "target": {"entity_id": "light.a"},
                   "return_response": true})
        );
    }

    #[test]
    fn parses_error_result() {
        let m: Incoming = serde_json::from_str(
            r#"{"id":5,"type":"result","success":false,"error":{"code":"invalid_format","message":"bad"}}"#,
        )
        .unwrap();
        assert_eq!(m.id, Some(5));
        assert!(matches!(Error::from(m.error.unwrap()), Error::Ha { code, .. } if code == "invalid_format"));
    }
}
