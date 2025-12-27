use anyhow::{anyhow, Context, Result};
use log::debug;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_tungstenite::tungstenite::Message;

use crate::client::connection::WsConnection;
use crate::client::events;
use crate::client::pending::{
    PendingCall, PendingGetHistoryCall, PendingServiceCall, PendingUnsubscribeCall,
};
use crate::client::protocol::{
    BaseMessage, CallServiceMessage, CommandMessage, EventMessage, GetHistoryMessage,
    IncomingMsgType, OutgoingMsgType, ResultMessage, SubscribeEntitiesMessage,
    SubscribeEventsMessage, UnsubscribeMessage,
};
use crate::client::state::HaState;
use crate::client::subscription::{self, HandlerCommand, HaEventSubscription, Subscription};
use crate::client::HAClient;

// --- Internal client runner that owns the state and connection ---

pub(crate) struct ClientRunner {
    pub(crate) conn: WsConnection,
    pub(crate) msg_id: AtomicU64,
    pub(crate) subscriptions: HashMap<u32, HaEventSubscription>,
    pub(crate) pending_one_shot_gets: HashMap<u64, (oneshot::Sender<Result<HaState>>, String)>,
    pub(crate) pending_api_calls: HashMap<u64, PendingCall>,
    pub(crate) shutdown_tx: Arc<watch::Sender<bool>>,
}

impl ClientRunner {
    pub(crate) async fn run_loop(
        mut self,
        mut command_rx: mpsc::Receiver<HandlerCommand>,
        mut shutdown_rx: watch::Receiver<bool>,
        client_command_tx: Arc<mpsc::Sender<HandlerCommand>>,
    ) -> Result<()> {
        log::info!("Starting client event loop...");

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        log::info!("Shutdown signal received.");
                        break;
                    }
                },
                Some(cmd) = command_rx.recv() => {
                    log::debug!("Received command in runner: {}", cmd.name());
                    if let Err(e) = self.handle_command(cmd).await {
                        log::error!("Failed to handle command: {}", e);
                    }
                },
                Some(ws_msg) = self.conn.recv_rx.recv() => {
                    match ws_msg {
                        Message::Text(text) => {
                            if text == "CONNECTED" {
                                if let Err(e) = self.resubscribe_all().await {
                                    log::error!("Failed to resubscribe: {}", e);
                                }
                                continue;
                            }
                            if text == "DISCONNECTED" {
                                log::warn!("WebSocket disconnected. Pending API calls will fail.");
                                self.pending_api_calls.clear();
                                self.pending_one_shot_gets.clear();
                                continue;
                            }
                            self.handle_ha_message(&text, client_command_tx.clone()).await;
                        }
                        Message::Close(_) => {
                            log::info!("WebSocket connection closed.");
                            break;
                        }
                        _ => {}
                    }
                },
                else => {
                    log::info!("Client channels closed. Shutting down.");
                    break
                },
            }
        }
        Ok(())
    }

    async fn handle_command(&mut self, cmd: HandlerCommand) -> Result<()> {
        let call_id = self.msg_id.fetch_add(1, Ordering::Relaxed);
        debug!("Handling command: {} with ID: {}", cmd.name(), call_id);

        let (pending_call, ha_message_str) = match cmd {
            HandlerCommand::Subscribe {
                event_type,
                filter,
                callback,
                responder,
            } => {
                debug!("Processed Subscribe command for ID: {}", call_id);
                let sub = HaEventSubscription::new(call_id as u32, event_type.clone(), filter, callback);
                self.subscriptions.insert(call_id as u32, sub);
                // Immediately confirm the subscription locally. The client can't do anything if this fails anyway.
                let _ = responder.send(Ok(Subscription { id: call_id as u32 }));

                let payload = SubscribeEventsMessage {
                    msg_type: OutgoingMsgType::SubscribeEvents,
                    event_type,
                };
                let msg = to_command_message_str(call_id, payload)?;
                (None, msg)
            }
            HandlerCommand::Unsubscribe {
                subscription_id,
                responder,
            } => {
                if self.subscriptions.remove(&subscription_id).is_some() {
                    debug!("Processed Unsubscribe command for ID: {}", call_id);
                    debug!("Unsubscribing from subscription id {}", subscription_id);
                    let pending = PendingCall::Unsubscribe(PendingUnsubscribeCall { responder });
                    let payload = UnsubscribeMessage {
                        msg_type: OutgoingMsgType::UnsubscribeEvents,
                        subscription: subscription_id as u64,
                    };
                    let msg = to_command_message_str(call_id, payload)?;
                    (Some(pending), msg)
                } else {
                    log::warn!(
                        "Attempted to unsubscribe from non-existent subscription id {}",
                        subscription_id
                    );
                    let _ = responder.send(Ok(()));
                    return Ok(());
                }
            }
            HandlerCommand::GetStateOnce {
                entity_id,
                responder,
            } => {
                debug!("Processed GetStateOnce command for ID: {}", call_id);
                self.pending_one_shot_gets
                    .insert(call_id, (responder, entity_id.clone()));
                let payload = SubscribeEntitiesMessage {
                    msg_type: OutgoingMsgType::SubscribeEntities,
                    entity_ids: vec![&entity_id],
                };
                let msg = to_command_message_str(call_id, payload)?;
                (None, msg)
            }
            HandlerCommand::CallService {
                domain,
                service,
                entity_id,
                service_data,
                responder,
            } => {
                debug!("Processed CallService command for ID: {}", call_id);
                let pending = PendingCall::ServiceCall(PendingServiceCall { responder });

                let mut final_service_data = service_data.unwrap_or_else(|| Value::Object(serde_json::Map::new()));
                if let Some(eid) = entity_id {
                    if let Some(map) = final_service_data.as_object_mut() {
                        map.insert("entity_id".to_string(), Value::String(eid));
                    }
                }

                let payload = CallServiceMessage {
                    msg_type: OutgoingMsgType::CallService,
                    domain: domain.to_str().to_string(),
                    service: service.to_str().to_string(),
                    service_data: Some(final_service_data),
                };
                let msg = to_command_message_str(call_id, payload)?;
                (Some(pending), msg)
            }
            HandlerCommand::GetHistory {
                entity_id,
                start_time,
                end_time,
                significant_changes_only,
                responder,
            } => {
                debug!("Processed GetHistory command for ID: {}", call_id);
                let pending = PendingCall::GetHistory(PendingGetHistoryCall {
                    entity_id: entity_id.clone(),
                    responder,
                });
                let payload = GetHistoryMessage {
                    msg_type: OutgoingMsgType::GetHistory,
                    entity_ids: vec![&entity_id],
                    start_time,
                    end_time: Some(end_time),
                    significant_changes_only: significant_changes_only.unwrap_or(false),
                };
                let msg = to_command_message_str(call_id, payload)?;
                (Some(pending), msg)
            }
        };

        self.conn
            .send_tx
            .send(Message::Text(ha_message_str.into()))
            .await
            .map_err(|_| anyhow!("Connection send channel closed"))?;

        if let Some(pending_call) = pending_call {
            self.pending_api_calls.insert(call_id, pending_call);
        }

        Ok(())
    }

    async fn resubscribe_all(&mut self) -> Result<()> {
        log::info!("WebSocket connected. Re-subscribing...");
        for (id, sub) in &self.subscriptions {
            let payload = SubscribeEventsMessage {
                msg_type: OutgoingMsgType::SubscribeEvents,
                event_type: sub.event_type.clone(),
            };
            let subscribe_msg_str = to_command_message_str(*id as u64, payload)?;
            self.conn
                .send_tx
                .send(Message::Text(subscribe_msg_str.into()))
                .await
                .context(format!("Failed to re-subscribe {}", id))?;
        }
        Ok(())
    }

    async fn handle_ha_message(
        &mut self,
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
                        self.handle_event_message(event_msg, handler_cmd_tx).await;
                    } else {
                        log::warn!("Failed to parse EventMessage: {}", text);
                    }
                }
                IncomingMsgType::Result => {
                    log::debug!("Handling Result message (ID: {:?}).", base.id);
                    if let Ok(result_msg) = serde_json::from_str(text) {
                        self.handle_result_message(result_msg).await;
                    } else {
                        log::warn!("Failed to parse ResultMessage: {}", text);
                    }
                }
                other => {
                    log::debug!("Received unhandled message type: {:?} (ID: {:?}).", other, base.id);
                }
            },
        }
    }

    async fn handle_event_message(
        &mut self,
        msg: EventMessage,
        handler_cmd_tx: Arc<mpsc::Sender<subscription::HandlerCommand>>,
    ) {
        log::debug!("Handling event message for ID: {}. Event type: {}", msg.id, msg.event.get("event_type").and_then(Value::as_str).unwrap_or("unknown"));

        // First, check if this is a one-shot subscription event
        if let Some((responder, entity_id)) = self.pending_one_shot_gets.remove(&msg.id) {
            log::debug!("Processing one-shot state event for entity: {}", entity_id);
            let event = msg.event;
            let result = (|| {
                let a_map = event
                    .get("a")
                    .ok_or_else(|| anyhow!("Event payload missing 'a' field"))?;

                if let Some(state_obj) = a_map.get(&entity_id) {
                    let state_str = state_obj
                        .get("s")
                        .and_then(Value::as_str)
                        .ok_or_else(|| anyhow!("Could not parse state from entity object"))?;
                    Ok(HaState::from_string(state_str))
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
            let unsubscribe_id = self.msg_id.fetch_add(1, Ordering::Relaxed);
            if let Ok(unsubscribe_msg) =
                to_command_message_str(unsubscribe_id, unsubscribe_payload)
            {
                if let Err(e) = self
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

        if let Some(sub) = self.subscriptions.get(&(msg.id as u32)) {
            log::debug!("Dispatching event to subscription callback for ID: {}", msg.id);
            let event_payload = &msg.event;
            // Filtering logic
            let should_trigger = match &sub.filter {
                subscription::EventFilter::Any => true,
                subscription::EventFilter::ByEntityId(id) => {
                    event_payload
                        .get("data")
                        .and_then(|d| d.get("entity_id"))
                        .and_then(Value::as_str)
                        == Some(id)
                }
                subscription::EventFilter::Custom(f) => f(event_payload),
            };

            if should_trigger {
                let client_for_callback = HAClient {
                    command_tx: Some(handler_cmd_tx.clone()),
                    shutdown_tx: Some(self.shutdown_tx.clone()),
                    task_handle: None,
                };
                let callback = sub.callback.clone();

                let event = match sub.event_type.as_str() {
                    "state_changed" => {
                        if let Some(data) = event_payload.get("data") {
                            match serde_json::from_value::<events::StateChangedData>(data.clone()) {
                                Ok(d) => events::Event::StateChanged {
                                    entity_id: d.entity_id,
                                    new_state: d.new_state,
                                    old_state: d.old_state,
                                },
                                Err(e) => {
                                    log::warn!("Failed to deserialize state_changed event data: {}. Falling back.", e);
                                    events::Event::Generic {
                                        event_type: sub.event_type.clone(),
                                        data: event_payload.clone(),
                                    }
                                }
                            }
                        } else {
                            log::warn!("'state_changed' event missing 'data' field. Falling back.");
                            events::Event::Generic {
                                event_type: sub.event_type.clone(),
                                data: event_payload.clone(),
                            }
                        }
                    }
                    _ => events::Event::Generic {
                        event_type: sub.event_type.clone(),
                        data: event_payload.clone(),
                    },
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

    async fn handle_result_message(&mut self, msg: ResultMessage<Value>) {
        log::debug!("Handling result message for ID: {}. Success: {}", msg.id, msg.success);
        // This is an ACK for a one-shot subscription. We can ignore it and wait for the event.
        if self.pending_one_shot_gets.contains_key(&msg.id) {
            log::debug!("Ignoring one-shot result ACK for ID: {}", msg.id);
            return;
        }

        if let Some(pending_call) = self.pending_api_calls.remove(&msg.id) {
            // Handle unsuccessful messages first
            if !msg.success {
                let error = anyhow!(
                    "Command (id: {}) failed: {}",
                    msg.id,
                    msg.error.unwrap_or_default()
                );
                log::error!("API call failed: {}", error);
                match pending_call {
                    PendingCall::GetStateOnce(_) => (),
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
                (PendingCall::GetStateOnce(_), _) => {
                    // This is handled by the event message, so we ignore the result ACK
                },
                (PendingCall::GetHistory(call), Some(payload)) => {
                    type HistoryResponse =
                        HashMap<String, Vec<events::HistoricalStateWire>>;

                    let response =
                        match serde_json::from_value::<HistoryResponse>(payload.clone()) {
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
                        PendingCall::GetStateOnce(_) => (), // Already handled
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
            log::debug!("Received result for unknown/expired call ID: {}", msg.id);
        }
    }
}

fn to_command_message_str<T: Serialize>(id: u64, payload: T) -> Result<String> {
    let msg = CommandMessage { id, payload };
    serde_json::to_string(&msg).context("Failed to serialize command message")
}