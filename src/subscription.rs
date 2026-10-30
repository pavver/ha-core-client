use serde_json::Value;
use serde_json::value::RawValue;
use tokio::sync::oneshot;

use crate::error::Result;
use crate::events::{State, SubEvent};
use crate::protocol::Request;
use crate::queue::QueueRx;

/// The error type callbacks return. Any `std::error::Error + Send + Sync` (including
/// [`Error`](crate::Error)) and `anyhow::Error` convert into it with `?`.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// What a subscription callback returns. Errors are logged, with their source chain.
pub type CallbackResult = std::result::Result<(), BoxError>;

/// A handle to an active subscription, used with `HAClient::unsubscribe` or
/// [`Session::unsubscribe`](crate::Session::unsubscribe).
///
/// The id is assigned by the client and stays the same across reconnects.
/// Dropping it does not unsubscribe; use [`guard`](Self::guard) for that.
/// It belongs to the client that created it: another client rejects it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Subscription {
    pub(crate) id: u64,
    /// [`Shared::client_id`](crate::runner::Shared) of the client that created it.
    pub(crate) client: u64,
}

impl Subscription {
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Wraps the subscription in a guard that unsubscribes when dropped.
    #[cfg(feature = "runtime")]
    pub fn guard(self, client: &crate::HAClient) -> SubscriptionGuard {
        SubscriptionGuard { client: client.clone(), subscription: Some(self) }
    }
}

/// Unsubscribes when dropped. Created by [`Subscription::guard`].
///
/// The guard holds an `HAClient` handle, so it keeps the client running.
#[cfg(feature = "runtime")]
#[must_use = "dropping the guard unsubscribes immediately"]
pub struct SubscriptionGuard {
    client: crate::HAClient,
    subscription: Option<Subscription>,
}

#[cfg(feature = "runtime")]
impl SubscriptionGuard {
    pub fn subscription(&self) -> &Subscription {
        self.subscription.as_ref().expect("present until dropped")
    }

    /// Unsubscribes now and waits for Home Assistant to confirm.
    pub async fn unsubscribe(mut self) -> Result<()> {
        let subscription = self.subscription.take().expect("present until dropped");
        self.client.unsubscribe(subscription).await
    }

    /// Releases the subscription without unsubscribing.
    pub fn into_inner(mut self) -> Subscription {
        self.subscription.take().expect("present until dropped")
    }
}

#[cfg(feature = "runtime")]
impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        if let Some(subscription) = self.subscription.take() {
            self.client.unsubscribe_detached(subscription);
        }
    }
}

/// Runs a subscription's callback for one event. The last argument is the subscription id
/// (for log messages); the conversion into the user's type happens inside, in the worker.
#[cfg(feature = "runtime")]
pub(crate) type EventCallback = std::sync::Arc<
    dyn Fn(crate::HAClient, SubEvent, u64) -> std::pin::Pin<Box<dyn std::future::Future<Output = CallbackResult> + Send>>
        + Send
        + Sync,
>;

/// How the events of a subscription reach the user.
pub(crate) enum Delivery {
    /// The queue's receiving end is handed to this function when the subscription is
    /// registered; `HAClient` starts a worker task that runs the callbacks.
    Worker(Box<dyn FnOnce(QueueRx) + Send>),
    /// The queue's receiving end is handed to the caller (an `EventStream`, or
    /// [`SessionEvents`](crate::SessionEvents)).
    Stream,
}

/// The answer to a `Subscribe` command.
pub(crate) struct Subscribed {
    pub(crate) subscription: Subscription,
    /// The queue of a [`Delivery::Stream`] subscription.
    pub(crate) rx: Option<QueueRx>,
}

/// Which updates of an entity `HAClient::subscribe_entity_with` delivers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum EntityChanges {
    /// Every update Home Assistant records: the state or any attribute changed.
    /// `old_state.state` may equal `new_state.state`.
    ///
    /// A sensor reporting an unchanged value with unchanged attributes is not an update:
    /// Home Assistant only refreshes `last_reported` and sends nothing to WebSocket clients;
    /// use `HAClient::watch_silence` to detect a sensor that stopped reporting.
    #[default]
    Any,
    /// Only changes of the state string (`on` → `off`, `21.5` → `22`); attribute-only
    /// updates are skipped, also when queued changes cancel out (`A → B → A`).
    State,
}

/// How the payloads of a subscription are turned into [`SubEvent`]s.
#[derive(Clone, Debug)]
pub(crate) enum SubKind {
    /// `subscribe_events`: bus events, reported as `SubEvent::Bus`.
    Events,
    /// `subscribe_events` for `state_changed`, reported as `SubEvent::StateChanged`.
    StateChangedEvents,
    /// `subscribe_events` for `call_service`, reported as `SubEvent::ServiceCall`.
    ServiceCallEvents,
    /// `subscribe_trigger` with a `state` trigger, reported as `SubEvent::StateChanged`.
    /// The entity's state is re-read after every reconnect. `merge`: the queue merges
    /// changes that pile up behind a slow callback (`Source::without_merging` turns it off).
    EntityState { entity_id: String, changes: EntityChanges, merge: bool },
    /// Any other `subscribe_trigger`, reported as `SubEvent::TriggerFired`.
    Trigger,
    /// `render_template`, reported as `SubEvent::TemplateRendered`.
    Template,
    /// The `render_template` of `watch_silence`, reported as `SubEvent::Silence`.
    Silence { entity_id: String },
    /// Any subscribe-style command given by the user, reported as `SubEvent::Raw`.
    Raw,
}

impl SubKind {
    pub(crate) fn convert(&self, payload: Value) -> SubEvent {
        match self {
            SubKind::Events => SubEvent::from_bus_event(payload),
            SubKind::StateChangedEvents => SubEvent::from_state_changed(payload),
            SubKind::ServiceCallEvents => SubEvent::from_service_call(payload),
            SubKind::EntityState { .. } => SubEvent::from_entity_trigger(payload),
            SubKind::Trigger => SubEvent::TriggerFired(crate::events::TriggerFired::from_payload(payload)),
            SubKind::Template => SubEvent::from_template(payload),
            SubKind::Silence { entity_id } => SubEvent::from_silence(entity_id, payload),
            SubKind::Raw => SubEvent::Raw(payload),
        }
    }
}

/// Messages from `HAClient` handles to the runner task.
pub(crate) enum Command {
    /// A request whose raw `result` is returned to the caller.
    Request {
        request: Request,
        responder: oneshot::Sender<Result<Box<RawValue>>>,
    },
    GetStateOnce {
        entity_id: String,
        responder: oneshot::Sender<Result<State>>,
    },
    Subscribe {
        kind: SubKind,
        request: Request,
        delivery: Delivery,
        responder: oneshot::Sender<Result<Subscribed>>,
    },
    Unsubscribe {
        id: u64,
        responder: oneshot::Sender<Result<Value>>,
    },
}
