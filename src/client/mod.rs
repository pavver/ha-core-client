pub mod connection;
pub mod context_helpers;
pub mod events;
pub mod handler;
pub mod pending;
pub mod state;
pub mod subscription;

use anyhow::Result;
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use connection::WsConnection;
use subscription::{EventCallback, HandlerCommand, HaEventSubscription};
use pending::{PendingCall, PendingGetStateCall, PendingServiceCall};

pub struct HAClient {
    conn: WsConnection,
    msg_id: AtomicU64,
    subscriptions: HashMap<u32, HaEventSubscription>,
}

impl HAClient {
    pub async fn new(url: &str, token: &str) -> Result<Self> {
        let conn = WsConnection::new(url.to_string(), token.to_string()).await?;
        Ok(Self {
            conn,
            msg_id: AtomicU64::new(1),
            subscriptions: HashMap::new(),
        })
    }

    pub async fn subscribe_entity(
        &mut self,
        entity_id: &str,
        callback: EventCallback,
    ) -> Result<()> {
        let id = self.msg_id.fetch_add(1, Ordering::Relaxed) as u32;
        let sub = HaEventSubscription::new_state_changed(id, entity_id, callback);
        
        let subscribe_msg = json!({
            "id": id,
            "type": "subscribe_events",
            "event_type": &sub.event_type,
        });

        self.conn.send_tx.send(Message::Text(subscribe_msg.to_string().into())).await?;
        info!("Subscribed to event type '{}' for entity '{}'", &sub.event_type, entity_id);

        self.subscriptions.insert(id, sub);
        Ok(())
    }

    pub async fn run(mut self) -> Result<()> {
        info!("Starting HAClient event loop...");
        
        let (handler_cmd_tx, mut handler_cmd_rx) = mpsc::channel::<HandlerCommand>(100);
        let mut pending_api_calls: HashMap<u64, PendingCall> = HashMap::new();

        loop {
            tokio::select! {
                Some(cmd) = handler_cmd_rx.recv() => {
                    let call_id = self.msg_id.fetch_add(1, Ordering::Relaxed);
                    
                    let (pending_call, ha_message) = match cmd {
                        HandlerCommand::GetState { entity_id, responder } => {
                            debug!("Processing get_state for '{}' with call_id {}", entity_id, call_id);
                            let pending = PendingCall::GetState(PendingGetStateCall { entity_id, responder });
                            let msg = json!({ "id": call_id, "type": "get_states" });
                            (pending, msg)
                        },
                        HandlerCommand::CallService { domain, service, entity_id, service_data, responder } => {
                            debug!("Processing call_service {}.{} with call_id {}", domain, service, call_id);
                            let pending = PendingCall::ServiceCall(PendingServiceCall { responder });
                            
                            // Merge entity_id into service_data if it exists
                            let mut final_service_data = service_data.unwrap_or_else(|| json!({}));
                            if let Some(eid) = entity_id {
                                if let Some(map) = final_service_data.as_object_mut() {
                                    map.insert("entity_id".to_string(), json!(eid));
                                } else {
                                    // If service_data is not an object, we create one.
                                    // This is unlikely but good to handle.
                                    final_service_data = json!({ "entity_id": eid });
                                }
                            }

                            let msg = json!({
                                "id": call_id,
                                "type": "call_service",
                                "domain": domain,
                                "service": service,
                                "service_data": final_service_data
                            });
                            (pending, msg)
                        }
                    };

                    if self.conn.send_tx.send(Message::Text(ha_message.to_string().into())).await.is_err() {
                        error!("Connection send channel closed, cannot process command.");
                        // The pending call is dropped here, which closes the oneshot, signaling an error.
                    } else {
                        pending_api_calls.insert(call_id, pending_call);
                    }
                },

                Some(ws_msg) = self.conn.recv_rx.recv() => {
                    match ws_msg {
                        Message::Text(text) => {
                            if text == "CONNECTED" {
                                info!("WebSocket connected. Re-subscribing...");
                                for (id, sub) in &self.subscriptions {
                                    let subscribe_msg = json!({
                                        "id": id,
                                        "type": "subscribe_events",
                                        "event_type": &sub.event_type,
                                    });
                                                                         if self.conn.send_tx.send(Message::Text(subscribe_msg.to_string().into())).await.is_err() {                                        error!("Failed to re-subscribe {}, connection send channel closed.", id);
                                    }
                                }
                                continue;
                            }
                            if text == "DISCONNECTED" {
                                warn!("WebSocket disconnected. Pending API calls will fail.");
                                pending_api_calls.clear();
                                continue;
                            }
                            self.handle_ha_message(&text, &handler_cmd_tx, &mut pending_api_calls).await;
                        }
                        Message::Close(_) => {
                            info!("WebSocket connection closed. Shutting down client.");
                            break;
                        }
                        _ => { /* Ignore Ping/Pong/Binary */ }
                    }
                },
                
                else => {
                    info!("Client channels closed. Shutting down.");
                    break
                },
            }
        }
        Ok(())
    }
}
