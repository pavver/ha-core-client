use super::events;
use super::state::HaState;
use anyhow::{anyhow, Result};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

// --- New definitions for handler context and commands ---

#[derive(Debug)]
pub enum HandlerCommand {
    GetState {
        entity_id: String,
        responder: oneshot::Sender<Result<Option<Value>>>,
    },
    CallService {
        domain: String,
        service: String,
        entity_id: Option<String>,
        service_data: Option<Value>,
        responder: oneshot::Sender<Result<()>>,
    },
}

#[derive(Clone)]
pub struct EventHandlerContext {
    pub(super) command_tx: mpsc::Sender<HandlerCommand>,
}

impl EventHandlerContext {
    pub fn new(command_tx: mpsc::Sender<HandlerCommand>) -> Self {
        Self { command_tx }
    }

    /// Fetches the state of an entity and returns it as a strongly-typed `HaState` enum.
    pub async fn get_state(&self, entity_id: &str) -> Result<HaState> {
        let (tx, rx) = oneshot::channel();
        let cmd = HandlerCommand::GetState {
            entity_id: entity_id.to_string(),
            responder: tx,
        };

        if self.command_tx.send(cmd).await.is_err() {
            return Err(anyhow!(
                "Failed to send command to client: command channel closed."
            ));
        }

        // Wait for the response from the client's event loop
        let response = match tokio::time::timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => return Err(anyhow!("Responder channel was dropped for get_state.")),
            Err(_) => {
                return Err(anyhow!(
                    "Request timed out waiting for state of {}",
                    entity_id
                ))
            }
        };

        // Convert the raw response into the HaState enum
        match response {
            Ok(Some(value)) => Ok(HaState::from_value(&value)),
            Ok(None) => Err(anyhow!("Entity '{}' not found or state is null.", entity_id)),
            Err(e) => Err(e),
        }
    }

    /// The core, generic function for calling any service. Helpers are preferred.
    pub async fn call_service(
        &self,
        domain: String,
        service: String,
        entity_id: Option<String>,
        service_data: Option<Value>,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let cmd = HandlerCommand::CallService {
            domain,
            service,
            entity_id,
            service_data,
            responder: tx,
        };

        if self.command_tx.send(cmd).await.is_err() {
            return Err(anyhow::anyhow!(
                "Failed to send command to client: command channel closed."
            ));
        }

        match tokio::time::timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(anyhow::anyhow!(
                "Responder channel was dropped for service call."
            )),
            Err(_) => Err(anyhow::anyhow!("Service call timed out.")),
        }
    }
}

// --- Modified EventCallback type for async handlers ---

// Now uses the type-safe `events::Event` enum and returns a Result
pub type EventCallback = Arc<
    dyn Fn(
            EventHandlerContext,
            events::Event,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>
        + Send
        + Sync,
>;

// --- Existing structs (unchanged logic, but uses new EventCallback type) ---

#[derive(Clone)]
pub struct HaEventSubscription {
    pub id: u32,             // внутрішній msg_id для WebSocket
    pub event_type: String,  // наприклад "state_changed" або власний тип
    pub filter: EventFilter, // логіка фільтрації
    pub callback: EventCallback,
}

#[derive(Clone)]
pub enum EventFilter {
    Any,
    ByEntityId(String),
    Custom(Arc<dyn Fn(&serde_json::Value) -> bool + Send + Sync>),
}

impl HaEventSubscription {
    pub fn new_state_changed(id: u32, entity_id: &str, callback: EventCallback) -> Self {
        HaEventSubscription {
            id,
            event_type: "state_changed".to_string(),
            filter: EventFilter::ByEntityId(entity_id.to_string()),
            callback,
        }
    }
}


