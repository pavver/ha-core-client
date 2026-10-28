use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::oneshot;

use crate::error::Result;
use crate::events::{Event, State};
use crate::protocol::Request;
use crate::HAClient;

/// A handle to an active subscription, used with [`HAClient::unsubscribe`].
///
/// The id is assigned by the client and stays the same across reconnects.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Subscription {
    pub(crate) id: u64,
}

impl Subscription {
    pub fn id(&self) -> u64 {
        self.id
    }
}

pub(crate) type EventCallback = Arc<
    dyn Fn(HAClient, Event) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
        + Send
        + Sync,
>;

pub(crate) fn boxed_callback<F, Fut>(callback: F) -> EventCallback
where
    F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    Arc::new(move |client, event| Box::pin(callback(client, event)))
}

/// How the payloads of a subscription are turned into [`Event`]s.
#[derive(Clone, Copy, Debug)]
pub(crate) enum SubKind {
    /// `subscribe_events`: bus events.
    Events,
    /// `subscribe_trigger` with a `state` trigger, reported as `Event::StateChanged`.
    EntityState,
    /// Any other `subscribe_trigger`, reported as `Event::TriggerFired`.
    Trigger,
}

impl SubKind {
    pub(crate) fn convert(self, payload: Value) -> Event {
        match self {
            SubKind::Events => Event::from_bus_event(payload),
            SubKind::EntityState => Event::from_entity_trigger(payload),
            SubKind::Trigger => Event::TriggerFired(crate::events::TriggerFiredEvent::from_payload(payload)),
        }
    }
}

/// Messages from [`HAClient`] handles to the runner task.
pub(crate) enum Command {
    /// A request whose raw `result` is returned to the caller.
    Request {
        request: Request,
        responder: oneshot::Sender<Result<Value>>,
    },
    GetStateOnce {
        entity_id: String,
        responder: oneshot::Sender<Result<State>>,
    },
    Subscribe {
        kind: SubKind,
        request: Request,
        callback: EventCallback,
        responder: oneshot::Sender<Result<Subscription>>,
    },
    Unsubscribe {
        id: u64,
        responder: oneshot::Sender<Result<Value>>,
    },
}
