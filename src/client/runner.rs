use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_tungstenite::tungstenite::Message;

use crate::client::connection::WsConnection;
use crate::client::handler;
use crate::client::pending::PendingCall;
use crate::client::protocol::{OutgoingMsgType, SubscribeEventsMessage, SubscribeTriggerMessage};
use crate::client::events;
use crate::client::subscription::{HandlerCommand, HaEventSubscription, InternalSubscriptionDetails};

// --- Internal client runner that owns the state and connection ---

pub(crate) struct ClientRunner {
    pub(crate) conn: WsConnection,
    pub(crate) msg_id: AtomicU64,
    pub(crate) subscriptions: HashMap<u32, HaEventSubscription>,
    pub(crate) pending_one_shot_gets: HashMap<u64, (oneshot::Sender<Result<events::State>>, String)>,
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
                    if let Err(e) = handler::command::handle_command(&mut self, cmd).await {
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
                            handler::incoming::handle_ha_message(&mut self, &text, client_command_tx.clone()).await;
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

    async fn resubscribe_all(&mut self) -> Result<()> {
        log::info!("WebSocket connected. Re-subscribing...");
        for (id, sub) in &self.subscriptions {
            let subscribe_msg_str = match &sub.details {
                InternalSubscriptionDetails::Event { event_type, .. } => {
                    let payload = SubscribeEventsMessage {
                        msg_type: OutgoingMsgType::SubscribeEvents,
                        event_type: event_type.clone(),
                    };
                    handler::command::to_command_message_str(*id as u64, payload)?
                }
                InternalSubscriptionDetails::Trigger { trigger } => {
                    let payload = SubscribeTriggerMessage {
                        msg_type: OutgoingMsgType::SubscribeTriggers,
                        trigger: trigger.clone(),
                    };
                    handler::command::to_command_message_str(*id as u64, payload)?
                }
            };
            self.conn
                .send_tx
                .send(Message::Text(subscribe_msg_str.into()))
                .await
                .context(format!("Failed to re-subscribe {}", id))?;
        }
        Ok(())
    }
}
