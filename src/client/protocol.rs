use serde::{Deserialize, Serialize};
use serde_json::Value;
use chrono::{DateTime, Utc};

// Outgoing Messages

#[derive(Debug, Serialize)]
pub(crate) struct AuthMessage<'a> {
    #[serde(rename = "type")]
    pub msg_type: OutgoingMsgType,
    pub access_token: &'a str,
}

#[derive(Debug, Serialize)]
pub(crate) struct CommandMessage<T> {
    pub id: u64,
    #[serde(flatten)]
    pub payload: T,
}

#[derive(Debug, Serialize)]
pub(crate) struct SubscribeEventsMessage {
    #[serde(rename = "type")]
    pub msg_type: OutgoingMsgType,
    pub event_type: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct SubscribeEntitiesMessage<'a> {
    #[serde(rename = "type")]
    pub msg_type: OutgoingMsgType,
    pub entity_ids: Vec<&'a str>,
}

#[derive(Debug, Serialize)]
pub(crate) struct UnsubscribeMessage {
    #[serde(rename = "type")]
    pub msg_type: OutgoingMsgType,
    pub subscription: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct CallServiceMessage {
    #[serde(rename = "type")]
    pub msg_type: OutgoingMsgType,
    pub domain: String,
    pub service: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_data: Option<Value>,
}

#[derive(Debug, Serialize)]
pub(crate) struct GetStatesMessage {
    #[serde(rename = "type")]
    pub msg_type: OutgoingMsgType,
}

#[derive(Debug, Serialize)]
pub(crate) struct GetHistoryMessage<'a> {
    #[serde(rename = "type")]
    pub msg_type: OutgoingMsgType,
    pub entity_ids: Vec<&'a str>,
    pub start_time: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time: Option<DateTime<Utc>>,
    pub significant_changes_only: bool,
}


// Incoming Messages

#[derive(Debug, Deserialize)]
pub(crate) struct BaseMessage {
    #[serde(rename = "type")]
    pub msg_type: IncomingMsgType,
    #[serde(default)]
    pub id: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct EventMessage {
    pub id: u64,
    pub event: Value,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ResultMessage<T> {
    pub id: u64,
    pub success: bool,
    pub result: Option<T>,
    pub error: Option<Value>,
}


#[derive(Debug, Serialize, PartialEq, Eq, Clone, Copy)]
pub(crate) enum OutgoingMsgType {
    #[serde(rename = "auth")]
    Auth,
    #[serde(rename = "subscribe_events")]
    SubscribeEvents,
    #[serde(rename = "subscribe_entities")]
    SubscribeEntities,
    #[serde(rename = "unsubscribe_events")]
    UnsubscribeEvents,
    #[serde(rename = "call_service")]
    CallService,
    #[serde(rename = "get_states")]
    GetStates,
    #[serde(rename = "history/history_during_period")]
    GetHistory,
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone, Copy)]
pub(crate) enum IncomingMsgType {
    #[serde(rename = "auth_required")]
    AuthRequired,
    #[serde(rename = "auth_ok")]
    AuthOk,
    #[serde(rename = "auth_invalid")]
    AuthInvalid,
    #[serde(rename = "event")]
    Event,
    #[serde(rename = "result")]
    Result,
}
