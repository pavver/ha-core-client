use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

/// The context of a Home Assistant state change, event or trigger: who or what caused it.
///
/// Deserialization never fails. The object form `{"id", "parent_id", "user_id"}` is accepted
/// with any field missing or `null`, and so is the bare id string that `subscribe_entities`
/// sends when there is no parent and no user. Anything else yields `Context::default()`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct Context {
    pub id: String,
    /// The id of the context that caused this one, e.g. the context of the event that
    /// triggered an automation or script.
    pub parent_id: Option<String>,
    /// The Home Assistant user whose action caused this change (UI, API or WebSocket call
    /// with that user's token).
    pub user_id: Option<String>,
}

impl Context {
    /// Whether a Home Assistant user directly caused the change (`user_id` is set).
    ///
    /// Home Assistant sets `user_id` for calls authenticated as a user. A change made by an
    /// automation that a user started has its own context with a `parent_id`, so check
    /// [`is_automated`](Self::is_automated) too to tell the two apart.
    pub fn is_user(&self) -> bool {
        self.user_id.is_some()
    }

    /// Whether the change was caused by an automation, script or other handler reacting to
    /// another event (`parent_id` is set).
    ///
    /// A change without a parent context originated at the root: a user action, a device
    /// or an integration polling.
    pub fn is_automated(&self) -> bool {
        self.parent_id.is_some()
    }

    fn from_value(value: Value) -> Self {
        match value {
            Value::String(id) => Self { id, ..Self::default() },
            Value::Object(mut map) => {
                let mut text = |key: &str| match map.remove(key) {
                    Some(Value::String(s)) => Some(s),
                    _ => None,
                };
                Self { id: text("id").unwrap_or_default(), parent_id: text("parent_id"), user_id: text("user_id") }
            }
            Value::Null => Self::default(),
            other => {
                log::debug!("Unexpected context shape, ignoring: {other}");
                Self::default()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Context {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::from_value(Value::deserialize(deserializer)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(v: Value) -> Context {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn object_form() {
        let c = parse(json!({"id": "01X", "parent_id": null, "user_id": null}));
        assert_eq!(c.id, "01X");
        assert!(c.parent_id.is_none() && c.user_id.is_none());
        assert!(!c.is_user() && !c.is_automated());

        let c = parse(json!({"id": "01Y", "parent_id": "01X", "user_id": "abc"}));
        assert_eq!((c.parent_id.as_deref(), c.user_id.as_deref()), (Some("01X"), Some("abc")));
        assert!(c.is_user() && c.is_automated());
    }

    #[test]
    fn missing_fields_default() {
        assert_eq!(parse(json!({"id": "01X"})).id, "01X");
        assert_eq!(parse(json!({})), Context::default());
    }

    #[test]
    fn string_form() {
        let c = parse(json!("01ABC"));
        assert_eq!(c.id, "01ABC");
        assert!(c.parent_id.is_none() && c.user_id.is_none());
    }

    #[test]
    fn null_and_garbage_become_default() {
        for v in [json!(null), json!(42), json!([1, 2]), json!(true), json!({"id": 5, "user_id": {"x": 1}})] {
            assert_eq!(parse(v), Context::default());
        }
    }
}
