use std::sync::atomic::Ordering;
use anyhow::{anyhow, Context, Result};
use log::debug;
use serde::Serialize;
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;

use crate::pending::{
    PendingCall, PendingGetHistoryCall, PendingGetStatesCall, PendingServiceCall,
    PendingUnsubscribeCall,
};
use crate::protocol::{
    CallServiceMessage, CommandMessage, GetHistoryMessage, OutgoingMsgType,
    SubscribeEntitiesMessage, SubscribeEventsMessage, UnsubscribeMessage,
};
use crate::runner::ClientRunner;
use crate::subscription::{HandlerCommand, HaEventSubscription, Subscription};

#[derive(Serialize)]
struct GetStatesPayload {
    #[serde(rename = "type")]
    msg_type: OutgoingMsgType,
}

pub(crate) async fn handle_command(
    runner: &mut ClientRunner,
    cmd: HandlerCommand,
) -> Result<()> {
    let call_id = runner.msg_id.fetch_add(1, Ordering::Relaxed);
    debug!("Handling command: {} with ID: {}", cmd.name(), call_id);

    let (pending_call, ha_message_str) = match cmd {
        HandlerCommand::Subscribe {
            details,
            callback,
            responder,
        } => {
            debug!("Processed Subscribe command for ID: {}", call_id);
            let sub = HaEventSubscription::new(call_id as u32, details.clone(), callback);
            runner.subscriptions.insert(call_id as u32, sub);
            // Immediately confirm the subscription locally. The client can't do anything if this fails anyway.
            let _ = responder.send(Ok(Subscription { id: call_id as u32 }));

            let msg = match details {
                crate::subscription::InternalSubscriptionDetails::Event { event_type, .. } => {
                    let payload = SubscribeEventsMessage {
                        msg_type: OutgoingMsgType::SubscribeEvents,
                        event_type,
                    };
                    to_command_message_str(call_id, payload)?
                }
                crate::subscription::InternalSubscriptionDetails::Trigger { trigger } => {
                    let payload = crate::protocol::SubscribeTriggerMessage {
                        msg_type: OutgoingMsgType::SubscribeTriggers,
                        trigger,
                    };
                    to_command_message_str(call_id, payload)?
                }
            };
            (None, msg)
        }
        HandlerCommand::Unsubscribe {
            subscription_id,
            responder,
        } => {
            if runner.subscriptions.remove(&subscription_id).is_some() {
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
            runner
                .pending_one_shot_gets
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

            let mut final_service_data =
                service_data.unwrap_or_else(|| Value::Object(serde_json::Map::new()));
            if let Some(eid) = entity_id {
                if let Some(map) = final_service_data.as_object_mut() {
                    map.insert("entity_id".to_string(), Value::String(eid));
                }
            }

            let payload = CallServiceMessage {
                msg_type: OutgoingMsgType::CallService,
                domain: domain.to_string(),
                service: service.to_string(),
                service_data: Some(final_service_data),
            };
            let msg = to_command_message_str(call_id, payload)?;
            (Some(pending), msg)
        }
        HandlerCommand::GetStates { responder } => {
            debug!("Processed GetStates command for ID: {}", call_id);
            let pending = PendingCall::GetStates(PendingGetStatesCall { responder });
            let payload = GetStatesPayload {
                msg_type: OutgoingMsgType::GetStates,
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

    runner
        .conn
        .send_tx
        .send(Message::Text(ha_message_str.into()))
        .await
        .map_err(|_| anyhow!("Connection send channel closed"))?;

    if let Some(pending_call) = pending_call {
        runner.pending_api_calls.insert(call_id, pending_call);
    }

    Ok(())
}


pub(crate) fn to_command_message_str<T: Serialize>(id: u64, payload: T) -> Result<String> {
    let msg = CommandMessage { id, payload };
    serde_json::to_string(&msg).context("Failed to serialize command message")
}
