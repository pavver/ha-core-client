//! Wire format of the Home Assistant WebSocket API.

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{json, Value};

use crate::error::{Error, HaErrorTranslation};
#[cfg(test)]
use crate::error::HaErrorCode;
use crate::target::Target;

/// A command body without its `id`; the runner assigns ids per connection.
pub(crate) type Request = Value;

/// Checks a user-supplied command: a JSON object with a string `type` and no `id`, and not
/// `auth` or `unsubscribe_events` (which would break the client's own bookkeeping).
pub(crate) fn user_request(request: impl serde::Serialize) -> Result<Request, Error> {
    let request = serde_json::to_value(request)
        .map_err(|e| Error::InvalidInput(format!("the request cannot be serialized: {e}")))?;
    let Some(object) = request.as_object() else {
        return Err(Error::InvalidInput(format!("a command must be a JSON object, got {request}")));
    };
    if object.contains_key("id") {
        return Err(Error::InvalidInput("a command must not contain `id`: the client assigns ids".into()));
    }
    match object.get("type").and_then(Value::as_str) {
        None => Err(Error::InvalidInput("a command needs a string field `type`".into())),
        Some("auth") => Err(Error::InvalidInput("`auth` is handled by the client".into())),
        Some("unsubscribe_events") => {
            Err(Error::InvalidInput("`unsubscribe_events` is not allowed; use HAClient::unsubscribe".into()))
        }
        Some(_) => Ok(request),
    }
}

/// Like [`user_request`] for a one-shot command: also rejects commands that create a
/// server-side subscription, whose events nobody would route.
pub(crate) fn user_command(request: impl serde::Serialize) -> Result<Request, Error> {
    let request = user_request(request)?;
    let kind = request["type"].as_str().unwrap_or_default();
    if kind.starts_with("subscribe_") || kind == "render_template" {
        return Err(Error::InvalidInput(format!(
            "`{kind}` creates a subscription; use Source::raw with HAClient::subscribe"
        )));
    }
    Ok(request)
}

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

pub(crate) fn render_template(template: &str) -> Request {
    json!({ "type": "render_template", "template": template, "report_errors": true })
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
    target: &Target,
    service_data: Option<Value>,
    return_response: bool,
) -> Request {
    let mut msg = json!({ "type": "call_service", "domain": domain, "service": service });
    if let Some(data) = service_data {
        msg["service_data"] = data;
    }
    if !target.is_empty() {
        msg["target"] = json!(target);
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
    pub result: Option<Box<RawValue>>,
    #[serde(default)]
    pub error: Option<HaError>,
    #[serde(default)]
    pub event: Option<Value>,
    /// Set on `auth_invalid`.
    #[serde(default)]
    pub message: Option<String>,
    /// Set on `auth_required` and `auth_ok`.
    #[serde(default)]
    pub ha_version: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct HaError {
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub message: String,
    // Kept as raw values so an odd shape cannot fail the whole message.
    #[serde(default)]
    pub translation_key: Value,
    #[serde(default)]
    pub translation_domain: Value,
    #[serde(default)]
    pub translation_placeholders: Value,
}

impl From<HaError> for Error {
    fn from(e: HaError) -> Self {
        let translation = match e.translation_key {
            Value::String(key) => Some(Box::new(HaErrorTranslation {
                domain: match e.translation_domain {
                    Value::String(domain) => Some(domain),
                    _ => None,
                },
                key,
                placeholders: match e.translation_placeholders {
                    Value::Object(map) => map,
                    _ => Default::default(),
                },
            })),
            _ => None,
        };
        Error::Ha { code: e.code.into(), message: e.message, translation }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_requests_are_validated() {
        assert!(user_request(json!({"type": "get_config"})).is_ok());
        assert!(user_request(json!({"type": "subscribe_events"})).is_ok());
        for bad in [
            json!({"event_type": "x"}),
            json!({"type": 5}),
            json!(["get_config"]),
            json!("get_config"),
            json!({"type": "get_config", "id": 3}),
            json!({"type": "auth", "access_token": "t"}),
            json!({"type": "unsubscribe_events", "subscription": 1}),
        ] {
            assert!(matches!(user_request(&bad), Err(Error::InvalidInput(_))), "{bad}");
        }
    }

    #[test]
    fn user_commands_reject_subscriptions() {
        assert!(user_command(json!({"type": "fire_event", "event_type": "x"})).is_ok());
        for bad in ["subscribe_events", "subscribe_trigger", "subscribe_custom", "render_template"] {
            assert!(matches!(user_command(json!({"type": bad})), Err(Error::InvalidInput(_))), "{bad}");
        }
    }

    #[test]
    fn incoming_keeps_result_verbatim() {
        let text = r#"{"id":3,"type":"result","success":true,"result":{"a":[1, 2],"b": {"c":null}}}"#;
        let msg: Incoming = serde_json::from_str(text).unwrap();
        assert_eq!(msg.result.unwrap().get(), r#"{"a":[1, 2],"b": {"c":null}}"#);
        let msg: Incoming = serde_json::from_str(r#"{"id":4,"type":"result","success":true}"#).unwrap();
        assert!(msg.result.is_none());
    }

    #[test]
    fn call_service_uses_target() {
        let v = call_service("light", "turn_on", &Target::entity("light.a"), Some(json!({"brightness": 10})), true);
        assert_eq!(
            v,
            json!({"type": "call_service", "domain": "light", "service": "turn_on",
                   "service_data": {"brightness": 10}, "target": {"entity_id": ["light.a"]},
                   "return_response": true})
        );
    }

    #[test]
    fn call_service_without_target_or_data() {
        let v = call_service("homeassistant", "reload", &Target::new(), None, false);
        assert_eq!(v, json!({"type": "call_service", "domain": "homeassistant", "service": "reload"}));
    }

    #[test]
    fn parses_error_result() {
        let m: Incoming = serde_json::from_str(
            r#"{"id":5,"type":"result","success":false,"error":{"code":"invalid_format","message":"bad"}}"#,
        )
        .unwrap();
        assert_eq!(m.id, Some(5));
        assert!(matches!(Error::from(m.error.unwrap()), Error::Ha { code, .. } if code == HaErrorCode::InvalidFormat));
    }

    fn parse_error(text: &str) -> Error {
        let m: Incoming = serde_json::from_str(text).unwrap();
        Error::from(m.error.unwrap())
    }

    #[test]
    fn error_carries_translation() {
        let err = parse_error(
            r#"{"id":7,"type":"result","success":false,"error":{"code":"service_validation_error",
            "message":"Validation error: Entity light.x does not support brightness",
            "translation_key":"entity_not_supported","translation_domain":"homeassistant",
            "translation_placeholders":{"entity_id":"light.x"}}}"#,
        );
        let Error::Ha { code, translation, .. } = err else { panic!("{err:?}") };
        assert_eq!(code, HaErrorCode::ServiceValidationError);
        let t = translation.unwrap();
        assert_eq!(t.domain.as_deref(), Some("homeassistant"));
        assert_eq!(t.key, "entity_not_supported");
        assert_eq!(t.placeholders["entity_id"], "light.x");
    }

    #[test]
    fn error_without_translation_or_with_odd_fields() {
        let err = parse_error(r#"{"id":7,"type":"result","success":false,"error":{"code":"not_found","message":"x"}}"#);
        assert!(matches!(err, Error::Ha { translation: None, .. }));
        // Odd types never fail the message: no usable key means no translation.
        let err = parse_error(
            r#"{"id":7,"type":"result","success":false,"error":{"code":"x","message":"m",
            "translation_key":5,"translation_domain":[],"translation_placeholders":"a"}}"#,
        );
        assert!(matches!(err, Error::Ha { translation: None, .. }));
        // A key with odd companions still yields a translation.
        let err = parse_error(
            r#"{"id":7,"type":"result","success":false,"error":{"code":"x","message":"m",
            "translation_key":"k","translation_domain":null,"translation_placeholders":"a"}}"#,
        );
        let Error::Ha { translation, .. } = err else { panic!() };
        let t = translation.unwrap();
        assert!(t.domain.is_none() && t.placeholders.is_empty());
    }
}
