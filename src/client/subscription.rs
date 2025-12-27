use super::events::{self, HistoricalState};
use super::state::{Domain, Service, HaState};
use anyhow::Result;
use chrono::{DateTime, Utc}; // Re-add
use serde_json::Value; // Re-add
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::oneshot;

// --- New definitions for handler context and commands ---

pub enum HandlerCommand {
    GetStateOnce {
        entity_id: String,
        responder: oneshot::Sender<Result<HaState>>,
    },
    CallService {
        domain: Domain,
        service: Service,
        entity_id: Option<String>,
        service_data: Option<Value>,
        responder: oneshot::Sender<Result<()>>,
    },
    GetHistory {
        entity_id: String,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        significant_changes_only: Option<bool>,
        responder: oneshot::Sender<Result<Vec<HistoricalState>>>,
    },
    Subscribe {
        event_type: String,
        filter: EventFilter,
        callback: EventCallback,
        responder: oneshot::Sender<Result<Subscription>>,
    },
    Unsubscribe {
        subscription_id: u32,
        responder: oneshot::Sender<Result<()>>,
    },
}

impl HandlerCommand {
    pub fn name(&self) -> &'static str {
        match self {
            HandlerCommand::GetStateOnce { .. } => "GetStateOnce",
            HandlerCommand::CallService { .. } => "CallService",
            HandlerCommand::GetHistory { .. } => "GetHistory",
            HandlerCommand::Subscribe { .. } => "Subscribe",
            HandlerCommand::Unsubscribe { .. } => "Unsubscribe",
        }
    }
}

/// A handle to a subscription that can be used to unsubscribe.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Subscription {
    pub id: u32,
}

// --- Modified EventCallback type for async handlers ---

// Now uses the type-safe `events::Event` enum and returns a Result
pub type EventCallback = Arc<
    dyn Fn(
            super::HAClient, // The first argument is now the client handle
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

impl std::fmt::Debug for EventFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventFilter::Any => write!(f, "Any"),
            EventFilter::ByEntityId(id) => f.debug_tuple("ByEntityId").field(id).finish(),
            EventFilter::Custom(_) => f.debug_tuple("Custom").field(&"<function>").finish(),
        }
    }
}

impl HaEventSubscription {
    pub fn new(
        id: u32,
        event_type: String,
        filter: EventFilter,
        callback: EventCallback,
    ) -> Self {
        HaEventSubscription {
            id,
            event_type,
            filter,
            callback,
        }
    }
}


