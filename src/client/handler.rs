use super::{
    events,
    pending::PendingCall,
    subscription::{self, EventHandlerContext},
    HAClient,
};
use anyhow::anyhow;
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

impl HAClient {
    pub(super) async fn handle_ha_message(
        &self,
        text: &str,
        handler_cmd_tx: &mpsc::Sender<subscription::HandlerCommand>,
        pending_api_calls: &mut HashMap<u64, PendingCall>,
    ) {
        match serde_json::from_str::<Value>(text) {
            Err(e) => {
                warn!("Failed to parse incoming JSON: {}", e);
                return;
            }
            Ok(msg) => {
                match msg.get("type").and_then(Value::as_str) {
                    Some("event") => {
                        self.handle_event_message(&msg, handler_cmd_tx).await;
                    }
                    Some("result") => {
                        self.handle_result_message(&msg, pending_api_calls).await;
                    }
                    _ => {
                        debug!("Received unhandled message type: {:?}", msg);
                    }
                }
            }
        }
    }

    async fn handle_event_message(
        &self,
        msg: &Value,
        handler_cmd_tx: &mpsc::Sender<subscription::HandlerCommand>,
    ) {
        if let Some(event_payload) = msg.get("event") {
            if let Some(sub_id) = msg.get("id").and_then(Value::as_u64) {
                if let Some(sub) = self.subscriptions.get(&(sub_id as u32)) {
                    // Filtering logic to see if this specific event should trigger the callback
                    let should_trigger = match &sub.filter {
                        subscription::EventFilter::Any => true,
                        subscription::EventFilter::ByEntityId(id) => event_payload
                            .get("data")
                            .and_then(|d| d.get("entity_id"))
                            .and_then(Value::as_str)
                            == Some(id),
                        subscription::EventFilter::Custom(f) => f(event_payload),
                    };

                    if should_trigger {
                        let context = EventHandlerContext::new(handler_cmd_tx.clone());
                        let callback = sub.callback.clone();

                        // --- New Deserialization Logic ---
                        // Try to parse the incoming JSON into a strongly-typed Event enum
                        let event = match sub.event_type.as_str() {
                            "state_changed" => {
                                if let Some(data) = event_payload.get("data") {
                                    match serde_json::from_value::<events::StateChangedData>(
                                        data.clone(),
                                    ) {
                                        Ok(d) => events::Event::StateChanged {
                                            entity_id: d.entity_id,
                                            new_state: d.new_state,
                                            old_state: d.old_state,
                                        },
                                        Err(e) => {
                                            warn!("Failed to deserialize state_changed event data: {}. Falling back to generic event.", e);
                                            events::Event::Generic {
                                                event_type: sub.event_type.clone(),
                                                data: event_payload.clone(),
                                            }
                                        }
                                    }
                                } else {
                                    warn!("'state_changed' event is missing 'data' field. Falling back to generic event.");
                                    events::Event::Generic {
                                        event_type: sub.event_type.clone(),
                                        data: event_payload.clone(),
                                    }
                                }
                            }
                            // --- Default case for other event types ---
                            _ => events::Event::Generic {
                                event_type: sub.event_type.clone(),
                                data: event_payload.clone(),
                            },
                        };

                        // The callback future now returns a Result.
                        // We spawn it and log if an error occurs.
                        let future = callback(context, event);
                        tokio::spawn(async move {
                            if let Err(e) = future.await {
                                error!("Event handler failed: {}", e);
                            }
                        });
                    }
                }
            }
        }
    }

    async fn handle_result_message(
        &self,
        msg: &Value,
        pending_api_calls: &mut HashMap<u64, PendingCall>,
    ) {
        if let Some(call_id) = msg.get("id").and_then(Value::as_u64) {
            if let Some(pending_call) = pending_api_calls.remove(&call_id) {
                // Check for command success first
                if !msg.get("success").and_then(Value::as_bool).unwrap_or(true) {
                    let error_payload = msg.get("error").cloned().unwrap_or_default();
                    let err = Err(anyhow!("Command failed: {}", error_payload));
                    match pending_call {
                        PendingCall::GetState(call) => {
                            let _ = call.responder.send(err.map(|_| None));
                        }
                        PendingCall::ServiceCall(call) => {
                            let _ = call.responder.send(err);
                        }
                    }
                    return;
                }

                // Handle successful result
                let result_payload = msg.get("result");

                match (pending_call, result_payload) {
                    (PendingCall::GetState(call), Some(payload)) => {
                        let response = payload.as_array().map_or_else(
                            || Err(anyhow!("'get_states' result payload is not an array")),
                            |states| {
                                Ok(states
                                    .iter()
                                    .find(|s| {
                                        s.get("entity_id").and_then(Value::as_str)
                                            == Some(&call.entity_id)
                                    })
                                    .and_then(|s| s.get("state").cloned()))
                            },
                        );
                        let _ = call.responder.send(response);
                    }
                    (PendingCall::ServiceCall(call), _result) => {
                        let _ = call.responder.send(Ok(()));
                    }
                    (_, None) => {
                        let err = Err(anyhow!(
                            "Successful 'result' message has no 'result' field"
                        ));
                        match pending_api_calls.remove(&call_id) {
                            Some(PendingCall::GetState(call)) => {
                                let _ = call.responder.send(err.map(|_| None));
                            }
                            Some(PendingCall::ServiceCall(call)) => {
                                let _ = call.responder.send(err);
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }
}
