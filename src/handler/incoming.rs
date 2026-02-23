use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use anyhow::{anyhow, Result};

use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use chrono::{DateTime, Utc};

use crate::events::{self, attributes::AttributeSet, state::{HistoricalStateWire, StateChangedData}, helpers::de_helpers};
use crate::pending::PendingCall;
use crate::protocol::{
    BaseMessage, EventMessage, IncomingMsgType, OutgoingMsgType, ResultMessage, UnsubscribeMessage,
};
use crate::runner::ClientRunner;
use crate::subscription;
use crate::HAClient;
use crate::handler::command::to_command_message_str;


// Temporary struct for parsing the one-shot state event
#[derive(Deserialize)]
struct OneShotStateWire {
    #[serde(rename = "s")]
    state: String,
    #[serde(rename = "a")]
    attributes: AttributeSet,
    #[serde(rename = "c")]
    context_id: String,
    #[serde(rename = "lc", deserialize_with = "de_helpers::deserialize_f64_unix_timestamp")]
    last_changed: DateTime<Utc>,
    #[serde(default)]
    #[serde(rename = "lu")]
    #[serde(deserialize_with = "de_helpers::deserialize_optional_f64_unix_timestamp")]
    last_updated: Option<DateTime<Utc>>,
}

fn parse_one_shot_state(entity_id: &str, state_obj: &Value) -> Result<events::State> {
    let wire: OneShotStateWire = serde_json::from_value(state_obj.clone())
        .map_err(|e| anyhow!("Failed to deserialize one-shot state object: {}", e))?;

    Ok(events::State {
        entity_id: entity_id.to_string(),
        state: wire.state,
        attributes: wire.attributes,
        last_changed: wire.last_changed,
        last_updated: wire.last_updated.unwrap_or(wire.last_changed),
        context: json!({ "id": wire.context_id }),
    })
}


pub(crate) async fn handle_ha_message(
    runner: &mut ClientRunner,
    text: &str,
    handler_cmd_tx: Arc<mpsc::Sender<subscription::HandlerCommand>>,
) {
    log::trace!("Raw incoming HA message: {}", text);
    log::debug!("Received HA message, attempting to parse.");
    match serde_json::from_str::<BaseMessage>(text) {
        Err(e) => {
            log::warn!("Failed to parse incoming JSON into BaseMessage: {}", e);
        }
        Ok(base) => match base.msg_type {
            IncomingMsgType::Event => {
                log::debug!("Handling Event message (ID: {:?}).", base.id);
                if let Ok(event_msg) = serde_json::from_str(text) {
                    handle_event_message(runner, event_msg, handler_cmd_tx).await;
                } else {
                    log::warn!("Failed to parse EventMessage: {}", text);
                }
            }
            IncomingMsgType::Result => {
                log::debug!("Handling Result message (ID: {:?}).", base.id);
                if let Ok(result_msg) = serde_json::from_str(text) {
                    handle_result_message(runner, result_msg).await;
                } else {
                    log::warn!("Failed to parse ResultMessage: {}", text);
                }
            }
            other => {
                log::debug!(
                    "Received unhandled message type: {:?} (ID: {:?}).",
                    other,
                    base.id
                );
            }
        },
    }
}

async fn handle_event_message(
    runner: &mut ClientRunner,
    msg: EventMessage,
    handler_cmd_tx: Arc<mpsc::Sender<subscription::HandlerCommand>>,
) {
    log::debug!(
        "Handling event message for ID: {}. Event type: {}",
        msg.id,
        msg.event
            .get("event_type")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
    );

    // First, check if this is a one-shot subscription event
    if let Some((responder, entity_id)) = runner.pending_one_shot_gets.remove(&msg.id) {
        log::debug!(
            "Processing one-shot state event for entity: {}",
            entity_id
        );
        let event = msg.event;
        let result = (|| {
            let a_map = event
                .get("a")
                .ok_or_else(|| anyhow!("Event payload missing 'a' field"))?;

            if let Some(state_obj) = a_map.get(&entity_id) {
                parse_one_shot_state(&entity_id, state_obj)
            } else {
                Err(anyhow!("Entity '{}' not found in response", entity_id))
            }
        })();

        if responder.send(result).is_err() {
            log::warn!("Failed to send one-shot state result: receiver was dropped");
        }

        // Unsubscribe immediately
        let unsubscribe_payload = UnsubscribeMessage {
            msg_type: OutgoingMsgType::UnsubscribeEvents,
            subscription: msg.id,
        };
        let unsubscribe_id = runner.msg_id.fetch_add(1, Ordering::Relaxed);
        if let Ok(unsubscribe_msg) = to_command_message_str(unsubscribe_id, unsubscribe_payload) {
            if let Err(e) = runner
                .conn
                .send_tx
                .send(Message::Text(unsubscribe_msg.into()))
                .await
            {
                log::warn!("Failed to send unsubscribe message: {}", e);
            }
        }

        return; // Stop processing, this was a one-shot event
    }

    if let Some(sub) = runner.subscriptions.get(&(msg.id as u32)) {
        log::debug!(
            "Dispatching event to subscription callback for ID: {}",
            msg.id
        );
        let event_payload = &msg.event;
        // Filtering logic
        let should_trigger = match &sub.details {
            subscription::InternalSubscriptionDetails::Event { filter, .. } => {
                match filter {
                    subscription::EventFilter::Any => true,
                    subscription::EventFilter::ByEntityId(id) => {
                        event_payload
                            .get("data")
                            .and_then(|d| d.get("entity_id"))
                            .and_then(Value::as_str)
                            == Some(id)
                    }
                    subscription::EventFilter::Custom(f) => f(event_payload),
                }
            }
            subscription::InternalSubscriptionDetails::Trigger { .. } => {
                // For trigger subscriptions, the filtering is handled by Home Assistant
                // when the trigger is defined, so we always dispatch the event if it
                // reaches here.
                true
            }
        };

        if should_trigger {
            let client_for_callback = HAClient {
                command_tx: Some(handler_cmd_tx.clone()),
                shutdown_tx: Some(runner.shutdown_tx.clone()),
                task_handle: None,
            };
            let callback = sub.callback.clone();

            let event = match &sub.details {
                subscription::InternalSubscriptionDetails::Event { event_type, .. } => match event_type.as_str() {
                    "state_changed" => {
                        if let Some(data) = event_payload.get("data") {
                            match serde_json::from_value::<StateChangedData>(data.clone()) {
                                Ok(d) => events::Event::StateChanged {
                                    entity_id: d.entity_id,
                                    new_state: d.new_state,
                                    old_state: d.old_state,
                                },
                                Err(e) => {
                                    log::warn!("Failed to deserialize state_changed event data: {}. Falling back.", e);
                                    events::Event::Generic {
                                        event_type: event_type.clone(),
                                        data: event_payload.clone(),
                                    }
                                }
                            }
                        } else {
                            log::warn!("'state_changed' event missing 'data' field. Falling back.");
                            events::Event::Generic {
                                event_type: event_type.clone(),
                                data: event_payload.clone(),
                            }
                        }
                    }
                    "call_service" => {
                        if let Some(data) = event_payload.get("data") {
                            match serde_json::from_value::<events::ServiceCalledEvent>(data.clone()) {
                                Ok(service_event) => events::Event::ServiceCalled(service_event),
                                Err(e) => {
                                    log::warn!("Failed to deserialize call_service event data: {}. Falling back.", e);
                                    events::Event::Generic {
                                        event_type: event_type.clone(),
                                        data: event_payload.clone(),
                                    }
                                }
                            }
                        } else {
                            log::warn!("'call_service' event missing 'data' field. Falling back.");
                            events::Event::Generic {
                                event_type: event_type.clone(),
                                data: event_payload.clone(),
                            }
                        }
                    }
                    _ => events::Event::Generic {
                        event_type: event_type.clone(),
                        data: event_payload.clone(),
                    },
                },
                subscription::InternalSubscriptionDetails::Trigger { .. } => {
                    match serde_json::from_value::<events::TriggerFiredEvent>(event_payload.clone()) {
                        Ok(trigger_event) => events::Event::TriggerFired(trigger_event),
                        Err(e) => {
                            log::warn!("Failed to deserialize trigger_fired event data: {}. Falling back.", e);
                            events::Event::Generic {
                                event_type: "trigger_fired".to_string(),
                                data: event_payload.clone(),
                            }
                        }
                    }
                }
            };

            let future = callback(client_for_callback, event);
            tokio::spawn(async move {
                if let Err(e) = future.await {
                    log::error!("Event handler failed: {}", e);
                }
            });
        }
    }
}

async fn handle_result_message(runner: &mut ClientRunner, msg: ResultMessage<Value>) {
    log::debug!(
        "Handling result message for ID: {}. Success: {}",
        msg.id,
        msg.success
    );
    // This is an ACK for a one-shot subscription. We can ignore it and wait for the event.
    if runner.pending_one_shot_gets.contains_key(&msg.id) {
        log::debug!("Ignoring one-shot result ACK for ID: {}", msg.id);
        return;
    }

    if let Some(pending_call) = runner.pending_api_calls.remove(&msg.id) {
        // Handle unsuccessful messages first
        if !msg.success {
            let error = anyhow!(
                "Command (id: {}) failed: {}",
                msg.id,
                msg.error.unwrap_or_default()
            );
            log::error!("API call failed: {}", error);
            match pending_call {
                PendingCall::GetStates(call) => {
                    let _ = call.responder.send(Err(error));
                }
                PendingCall::ServiceCall(call) => {
                    let _ = call.responder.send(Err(error));
                }
                PendingCall::GetHistory(call) => {
                    let _ = call.responder.send(Err(error));
                }
                PendingCall::Unsubscribe(call) => {
                    let _ = call.responder.send(Err(error));
                }
            }
            return;
        }

        // Handle successful messages
        log::debug!("API call (id: {}) successful.", msg.id);
        match (pending_call, msg.result) {
            (PendingCall::GetStates(call), Some(payload)) => {
                let response = match serde_json::from_value::<Vec<events::State>>(payload.clone()) {
                    Ok(states) => Ok(states),
                    Err(e) => {
                        log::error!(
                            "Failed to deserialize payload for GetStates: {}. Payload was: {}",
                            e,
                            serde_json::to_string_pretty(&payload).unwrap_or_default()
                        );
                        Err(anyhow!("Failed to deserialize states payload: {}", e))
                    }
                };
                let _ = call.responder.send(response);
            }
            (PendingCall::GetHistory(call), Some(payload)) => {
                type HistoryResponse = HashMap<String, Vec<HistoricalStateWire>>;

                let response = match serde_json::from_value::<HistoryResponse>(payload.clone()) {
                    Ok(history_map) => {
                        let states = history_map
                            .values()
                            .next()
                            .cloned()
                            .unwrap_or_default()
                            .into_iter()
                            .map(|wire| events::HistoricalState {
                                entity_id: call.entity_id.clone(),
                                state: wire.state,
                                attributes: wire.attributes,
                                last_updated: wire.last_updated,
                            })
                            .collect();
                        Ok(states)
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to deserialize history payload for GetHistory: {}. Payload was: {}",
                            e,
                            serde_json::to_string_pretty(&payload).unwrap_or_default()
                        );
                        Err(anyhow!("Failed to deserialize history payload: {}", e))
                    }
                };
                let _ = call.responder.send(response);
            }
            (PendingCall::ServiceCall(call), _) => {
                let _ = call.responder.send(Ok(()));
            }
            (PendingCall::Unsubscribe(call), _) => {
                let _ = call.responder.send(Ok(()));
            }
            (call, None) => {
                // This case handles successful results that are missing an expected payload.
                let error = anyhow!(
                    "Successful result for id {} had no payload where one was expected.",
                    msg.id
                );
                log::warn!("{}", error);
                match call {
                    PendingCall::GetStates(c) => {
                        let _ = c.responder.send(Err(error));
                    }
                    PendingCall::ServiceCall(c) => {
                        // Service calls may not have a result, so this is OK
                        let _ = c.responder.send(Ok(()));
                    }
                    PendingCall::GetHistory(c) => {
                        let _ = c.responder.send(Err(error));
                    }
                    PendingCall::Unsubscribe(c) => {
                        // Unsubscribe calls do not have a result, so this is OK
                        let _ = c.responder.send(Ok(()));
                    }
                }
            }
        }
    } else {
        log::debug!(
            "Received result for unknown/expired call ID: {}",
            msg.id
        );
    }
}
