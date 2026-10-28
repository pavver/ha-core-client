//! A strongly-typed, resilient asynchronous client for the Home Assistant WebSocket API.
//!
//! ```no_run
//! use ha_core_client::{HAClient, events::Event};
//!
//! # async fn example() -> ha_core_client::Result<()> {
//! let client = HAClient::connect("ws://homeassistant.local:8123/api/websocket", "TOKEN").await?;
//!
//! client.subscribe_entity("binary_sensor.motion", |client, event| async move {
//!     if let Event::StateChanged { new_state: Some(state), .. } = event {
//!         if state.state == "on" {
//!             client.turn_on("light.hallway").await?;
//!         }
//!     }
//!     Ok(())
//! }).await?;
//!
//! // Runs until `stop()` is called or authentication fails permanently.
//! client.closed().await;
//! # Ok(())
//! # }
//! ```

mod config;
mod connection;
mod context_helpers;
mod error;
pub mod events;
mod protocol;
mod queue;
mod runner;
pub mod state;
mod subscription;
pub mod triggers;

pub use config::ClientConfig;
pub use error::{Error, Result};
pub use events::SubscriptionStatus;
pub use subscription::{BoxError, CallbackResult, EntityChanges, Subscription, SubscriptionGuard};

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use log::debug;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, watch};

use crate::events::{Event, HistoricalState, State};
use crate::runner::{Runner, Shared};
use crate::state::{Domain, Service};
use crate::subscription::{boxed_callback, Command, SubKind};

/// How long [`HAClient::stop`] waits for callbacks to handle their queued events.
const STOP_CALLBACKS_TIMEOUT: Duration = Duration::from_secs(5);

/// Connection status of an [`HAClient`], see [`HAClient::watch_connection_state`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionState {
    /// Connected and authenticated; subscriptions are active.
    Connected,
    /// The connection was lost; requests fail with [`Error::NotConnected`] until it is back.
    /// `attempt` counts reconnects since the last session that stayed up (for the longest
    /// backoff delay, at most a minute), so a connection that keeps dropping right after
    /// connecting backs off instead of retrying at the shortest delay.
    Reconnecting { attempt: u32 },
    /// Home Assistant rejected the token while reconnecting. Terminal.
    AuthFailed(String),
    /// The client was stopped. Terminal.
    Stopped,
}

impl ConnectionState {
    /// `true` for states the client never leaves.
    pub fn is_terminal(&self) -> bool {
        matches!(self, ConnectionState::AuthFailed(_) | ConnectionState::Stopped)
    }
}

/// The handle for interacting with the Home Assistant WebSocket API.
///
/// `HAClient` is cheap to clone; every clone talks to the same background task,
/// which owns the connection, reconnects with backoff and restores subscriptions.
/// The task stops when [`stop`](Self::stop) is called or when every handle is dropped.
/// Handles captured by callbacks count too; callbacks don't need to capture one, since
/// they receive their own handle while they run.
#[derive(Clone)]
pub struct HAClient {
    cmd_tx: mpsc::Sender<Command>,
    shared: Arc<Shared>,
}

impl HAClient {
    /// Connects and authenticates with default settings.
    ///
    /// The first connection is not retried: a wrong URL or token is reported here
    /// (as [`Error::Connection`] or [`Error::Auth`]). Later disconnects are retried
    /// automatically. A URL that is not `ws://` or `wss://` (e.g. `http://host:8123`)
    /// fails with [`Error::InvalidInput`].
    pub async fn connect(url: impl Into<String>, token: impl Into<String>) -> Result<Self> {
        Self::connect_with(ClientConfig::new(url, token)).await
    }

    /// Connects and authenticates with custom settings. See [`connect`](Self::connect).
    pub async fn connect_with(config: ClientConfig) -> Result<Self> {
        connection::check_url(&config.url)?;
        let ws = connection::connect_and_auth(&config).await?;

        let (cmd_tx, cmd_rx) = mpsc::channel(256);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (state_tx, _) = watch::channel(ConnectionState::Connected);
        let (workers, _) = watch::channel(0);
        let shared = Arc::new(Shared { config, shutdown_tx, state_tx, workers });

        let runner = Runner::new(shared.clone(), cmd_tx.downgrade());
        tokio::spawn(runner.run(ws, cmd_rx, shutdown_rx));

        Ok(Self { cmd_tx, shared })
    }

    /// The current connection state.
    pub fn connection_state(&self) -> ConnectionState {
        self.shared.state_tx.borrow().clone()
    }

    /// A receiver that is notified on every connection state change.
    pub fn watch_connection_state(&self) -> watch::Receiver<ConnectionState> {
        self.shared.state_tx.subscribe()
    }

    /// Waits until the client reaches a terminal state and returns it.
    pub async fn closed(&self) -> ConnectionState {
        let mut rx = self.watch_connection_state();
        let state = rx.wait_for(ConnectionState::is_terminal).await.map(|s| s.clone());
        state.unwrap_or(ConnectionState::Stopped)
    }

    /// Stops the background task (closing the connection) and waits for it to finish.
    /// Affects every clone of this handle.
    ///
    /// Then waits (up to 5s) until the callbacks have handled the events already queued,
    /// ending with [`SubscriptionStatus::Closed`]. Called from inside a callback, it
    /// does not wait for callbacks, since that would wait for itself.
    pub async fn stop(&self) {
        debug!("Stopping HAClient...");
        self.shared.shutdown_tx.send_replace(true);
        self.closed().await;
        if runner::in_callback() {
            return;
        }
        let mut workers = self.shared.workers.subscribe();
        if tokio::time::timeout(STOP_CALLBACKS_TIMEOUT, workers.wait_for(|n| *n == 0)).await.is_err() {
            log::warn!("Subscription callbacks still running {STOP_CALLBACKS_TIMEOUT:?} after stop; not waiting for them.");
        }
    }

    async fn call<T>(&self, make: impl FnOnce(oneshot::Sender<Result<T>>) -> Command) -> Result<T> {
        if self.connection_state().is_terminal() {
            return Err(Error::Stopped);
        }
        let (tx, rx) = oneshot::channel();
        // Covers waiting for room in the command queue too, e.g. while the runner is busy.
        // A little slack so that the runner's own expiry usually answers first.
        let timeout = self.shared.config.request_timeout.saturating_add(Duration::from_secs(1));
        let call = async {
            self.cmd_tx.send(make(tx)).await.map_err(|_| Error::Stopped)?;
            rx.await.map_err(|_| Error::Stopped)?
        };
        tokio::time::timeout(timeout, call).await.unwrap_or(Err(Error::Timeout))
    }

    async fn request(&self, request: protocol::Request) -> Result<Value> {
        self.call(|responder| Command::Request { request, responder }).await
    }

    // --- States ---

    /// Fetches the current state of a specific entity.
    ///
    /// Fails with [`Error::Ha`] (code `not_found`) if the entity does not exist.
    pub async fn get_state(&self, entity_id: &str) -> Result<State> {
        debug!("Requesting state for entity: {}", entity_id);
        let entity_id = entity_id.to_string();
        self.call(|responder| Command::GetStateOnce { entity_id, responder }).await
    }

    /// Fetches the current state of all entities.
    ///
    /// Entities whose state cannot be parsed are skipped with a warning.
    pub async fn get_states(&self) -> Result<Vec<State>> {
        debug!("Requesting all states");
        let result = self.request(protocol::get_states()).await?;
        let Value::Array(items) = result else {
            return Err(Error::Protocol("get_states did not return a list".into()));
        };
        Ok(items
            .into_iter()
            .filter_map(|item| {
                serde_json::from_value::<State>(item)
                    .inspect_err(|e| log::warn!("Skipping entity with unparsable state: {e}"))
                    .ok()
            })
            .collect())
    }

    /// Fetches the history of states for a specific entity within a time range.
    ///
    /// Entries that cannot be parsed are skipped with a warning.
    pub async fn get_history(
        &self,
        entity_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        significant_changes_only: bool,
    ) -> Result<Vec<HistoricalState>> {
        let request = protocol::history_during_period(entity_id, start_time, end_time, significant_changes_only);
        let result = self.request(request).await?;
        Ok(parse_history(result, entity_id)?)
    }

    /// Finds the state of an entity at a specific point in time in the past.
    ///
    /// This method queries the history API within a `lookback` window
    /// ending at `timestamp` and returns the latest state recorded
    /// at or before that timestamp.
    pub async fn get_state_at_time(
        &self,
        entity_id: &str,
        timestamp: DateTime<Utc>,
        lookback: TimeDelta,
    ) -> Result<State> {
        let history = self.get_history(entity_id, timestamp - lookback, timestamp, true).await?;
        history
            .into_iter()
            .filter(|state| state.last_updated <= timestamp)
            .max_by_key(|state| state.last_updated)
            .map(Into::into)
            .ok_or_else(|| Error::Ha {
                code: "not_found".into(),
                message: format!("No state for '{entity_id}' in the window ending at {timestamp}"),
            })
    }

    // --- Services ---

    /// Calls a Home Assistant service, optionally targeting an entity.
    ///
    /// Accepts typed values or plain strings: `call_service(Domain::Light, Service::TurnOn, ...)`
    /// or `call_service("vacuum", "start", ...)`.
    pub async fn call_service(
        &self,
        domain: impl Into<Domain>,
        service: impl Into<Service>,
        entity_id: Option<&str>,
        service_data: Option<Value>,
    ) -> Result<()> {
        self.service_call(domain.into(), service.into(), entity_id, service_data, false).await.map(drop)
    }

    /// Calls a service that returns data (e.g. `weather.get_forecasts`) and returns its response.
    pub async fn call_service_with_response(
        &self,
        domain: impl Into<Domain>,
        service: impl Into<Service>,
        entity_id: Option<&str>,
        service_data: Option<Value>,
    ) -> Result<Value> {
        let mut result = self.service_call(domain.into(), service.into(), entity_id, service_data, true).await?;
        Ok(result.get_mut("response").map(Value::take).unwrap_or(Value::Null))
    }

    async fn service_call(
        &self,
        domain: Domain,
        service: Service,
        entity_id: Option<&str>,
        service_data: Option<Value>,
        return_response: bool,
    ) -> Result<Value> {
        debug!("Calling service: {}.{} for entity: {:?}", domain, service, entity_id);
        let request =
            protocol::call_service(domain.as_str(), service.as_str(), entity_id, service_data, return_response);
        self.request(request).await
    }

    // --- Subscriptions ---

    /// While the client is reconnecting, subscribing succeeds right away: the subscription
    /// starts with `SubscriptionStatus::Interrupted` and is sent once the connection is back
    /// (`Restored`, or `Rejected` if Home Assistant refuses it). The existence check of
    /// `subscribe_entity` is skipped in that case.
    async fn subscribe_with<F, Fut>(&self, kind: SubKind, request: protocol::Request, callback: F) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        let callback = boxed_callback(callback);
        self.call(|responder| Command::Subscribe { kind, request, callback, responder }).await
    }

    /// Subscribes to changes of one entity (state and attributes).
    ///
    /// The callback receives [`Event::StateChanged`]. Filtering happens in Home Assistant,
    /// so only this entity's changes are transferred. Callbacks of one subscription run
    /// one after another, in order.
    ///
    /// Fails with [`Error::Ha`] (code `not_found`) if the entity does not exist, which
    /// catches typos. To wait for an entity that does not exist yet, use
    /// [`subscribe_trigger`](Self::subscribe_trigger) with a [`StateTrigger`](triggers::StateTrigger) instead.
    ///
    /// After a reconnect the entity's state is re-read: if it changed while disconnected,
    /// the callback gets one `StateChanged` from the last known to the current state,
    /// followed by [`Event::Status`] with [`SubscriptionStatus::Restored`].
    ///
    /// Same as [`subscribe_entity_with`](Self::subscribe_entity_with) with [`EntityChanges::Any`].
    pub async fn subscribe_entity<F, Fut>(&self, entity_id: &str, callback: F) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        self.subscribe_entity_with(entity_id, EntityChanges::Any, callback).await
    }

    /// Like [`subscribe_entity`](Self::subscribe_entity), choosing which updates arrive:
    /// [`EntityChanges::State`] for changes of the state only, [`EntityChanges::Any`] for
    /// attribute updates too.
    pub async fn subscribe_entity_with<F, Fut>(
        &self,
        entity_id: &str,
        changes: EntityChanges,
        callback: F,
    ) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        debug!("Subscribing to entity: {} ({:?})", entity_id, changes);
        let mut trigger = serde_json::json!({ "trigger": "state", "entity_id": entity_id });
        if changes == EntityChanges::State {
            // `to: null` makes Home Assistant skip attribute-only changes.
            trigger["to"] = Value::Null;
        }
        let kind = SubKind::EntityState { entity_id: entity_id.to_string(), changes };
        self.subscribe_with(kind, protocol::subscribe_trigger(&trigger), callback).await
    }

    /// Subscribes to bus events of one type (`state_changed`, `call_service`, custom events, …).
    ///
    /// Note that `state_changed` delivers every state change in the system; prefer
    /// [`subscribe_entity`](Self::subscribe_entity) for single entities.
    pub async fn subscribe_events<F, Fut>(&self, event_type: &str, callback: F) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        debug!("Subscribing to events: {}", event_type);
        self.subscribe_with(SubKind::Events, protocol::subscribe_events(event_type), callback).await
    }

    /// Subscribes to all `call_service` events on the Home Assistant event bus.
    pub async fn subscribe_service_calls<F, Fut>(&self, callback: F) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        self.subscribe_events("call_service", callback).await
    }

    /// Subscribes to a Home Assistant trigger. The callback receives [`Event::TriggerFired`].
    ///
    /// Accepts a [`triggers::Trigger`] or any of its platform structs, e.g.
    /// `StateTrigger::new("binary_sensor.door").to(HaState::On).for_duration(..)`.
    ///
    /// Fails with [`Error::Ha`] if Home Assistant rejects the trigger configuration.
    pub async fn subscribe_trigger<F, Fut>(
        &self,
        trigger: impl Into<triggers::Trigger>,
        callback: F,
    ) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        let trigger = trigger.into();
        debug!("Subscribing to trigger: {:?}", trigger);
        let trigger = serde_json::to_value(trigger)?;
        self.subscribe_trigger_raw(trigger, callback).await
    }

    /// Subscribes to a trigger given as raw JSON, for trigger platforms and options
    /// not modelled by [`triggers::Trigger`].
    pub async fn subscribe_trigger_raw<F, Fut>(&self, trigger: Value, callback: F) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        self.subscribe_with(SubKind::Trigger, protocol::subscribe_trigger(&trigger), callback).await
    }

    /// Subscribes to a template rendered by Home Assistant. The callback receives
    /// [`Event::TemplateRendered`] with the current result right after subscribing, then
    /// each time the result changes, and [`Event::TemplateError`] if a later render fails.
    ///
    /// Home Assistant re-renders when a referenced entity changes, and at the start of
    /// every minute if the template uses `now()`. Results are parsed like in automations
    /// (`"{{ 1 + 1 }}"` gives the number `2`). A syntax error fails the subscription with
    /// [`Error::Ha`]; a render that fails (`{{ 1 / 0 }}`) arrives as `TemplateError`.
    ///
    /// After a reconnect the template is rendered again, so the current result follows
    /// [`SubscriptionStatus::Restored`], also if it did not change.
    pub async fn subscribe_template<F, Fut>(&self, template: &str, callback: F) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        debug!("Subscribing to template: {}", template);
        self.subscribe_with(SubKind::Template, protocol::render_template(template), callback).await
    }

    /// Watches whether an entity keeps reporting: the callback receives [`Event::Silence`]
    /// with `silent: true` once it has not reported for longer than `max_silence`, and
    /// `silent: false` when it reports again. The current value arrives right after
    /// subscribing and after every reconnect (following `Restored`).
    ///
    /// A report counts even if the value did not change (`last_reported`), which a state
    /// subscription cannot see. The check runs in Home Assistant, on its clock, when the
    /// entity changes and at the start of every minute, so `silent: true` comes up to a
    /// minute late, and so does `silent: false` when the entity resumes with an unchanged
    /// value. An entity that does not exist (or was removed) is silent.
    ///
    /// Fails with [`Error::InvalidInput`] for a malformed entity id.
    pub async fn watch_silence<F, Fut>(&self, entity_id: &str, max_silence: Duration, callback: F) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        let template = silence_template(entity_id, max_silence)?;
        debug!("Watching silence of {} ({:?})", entity_id, max_silence);
        let kind = SubKind::Silence { entity_id: entity_id.to_string() };
        self.subscribe_with(kind, protocol::render_template(&template), callback).await
    }

    /// Cancels a subscription. Events already queued for its callback are still delivered,
    /// followed by [`SubscriptionStatus::Unsubscribed`].
    pub async fn unsubscribe(&self, subscription: Subscription) -> Result<()> {
        debug!("Unsubscribing from subscription ID: {}", subscription.id);
        let id = subscription.id;
        self.call(|responder| Command::Unsubscribe { id, responder }).await.map(drop)
    }

    /// Unsubscribes without waiting, for `SubscriptionGuard::drop`.
    pub(crate) fn unsubscribe_detached(&self, subscription: Subscription) {
        let id = subscription.id;
        // Nobody waits for the answer.
        let (responder, _) = oneshot::channel();
        match self.cmd_tx.try_send(Command::Unsubscribe { id, responder }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(cmd)) => match tokio::runtime::Handle::try_current() {
                Ok(runtime) => {
                    let cmd_tx = self.cmd_tx.clone();
                    runtime.spawn(async move { cmd_tx.send(cmd).await });
                }
                Err(_) => log::warn!("Could not unsubscribe subscription {id}: command queue is full"),
            },
            // The client is stopped; its subscriptions are gone already.
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
}

/// Parses the `history_during_period` result for `entity_id`, skipping entries that do
/// not parse, so one bad record does not lose the whole history.
fn parse_history(result: Value, entity_id: &str) -> serde_json::Result<Vec<HistoricalState>> {
    let mut by_entity: HashMap<String, Vec<Value>> = serde_json::from_value(result)?;
    Ok(by_entity
        .remove(entity_id)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|item| {
            serde_json::from_value::<events::state::HistoricalStateWire>(item)
                .inspect_err(|e| log::warn!("Skipping unparsable history entry of {entity_id}: {e}"))
                .ok()
        })
        .map(|wire| wire.into_state(entity_id))
        .collect())
}

/// The template of [`HAClient::watch_silence`]: renders `true` while the entity is missing
/// or its last report is older than `max_silence`. The id is checked because it is part of
/// the template. The object id is subscripted, since `states.sensor.1st_floor` is a
/// Jinja syntax error.
fn silence_template(entity_id: &str, max_silence: Duration) -> Result<String> {
    let part_ok = |p: &str| !p.is_empty() && p.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    match entity_id.split_once('.') {
        Some((domain, object_id)) if part_ok(domain) && part_ok(object_id) => Ok(format!(
            "{{% set s = states.{domain}['{object_id}'] %}}{{{{ s is none or (now() - s.last_reported).total_seconds() > {} }}}}",
            max_silence.as_secs_f64()
        )),
        _ => Err(Error::InvalidInput(format!("invalid entity_id: {entity_id}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_skips_bad_entries() {
        let result = serde_json::json!({"sensor.t": [
            {"s": "1", "a": {}, "lu": 100.0},
            {"s": "2", "lu": "not a timestamp"},
            {"s": "3", "lu": 300.0}
        ]});
        let states: Vec<_> = parse_history(result, "sensor.t").unwrap().into_iter().map(|h| h.state).collect();
        assert_eq!(states, ["1", "3"]);
        assert!(parse_history(serde_json::json!({}), "sensor.t").unwrap().is_empty());
    }

    #[test]
    fn silence_template_checks_the_entity_id() {
        assert_eq!(
            silence_template("sensor.outdoor_2", Duration::from_secs(1800)).unwrap(),
            "{% set s = states.sensor['outdoor_2'] %}{{ s is none or (now() - s.last_reported).total_seconds() > 1800 }}"
        );
        // Object ids may start with a digit.
        assert!(silence_template("sensor.1st_floor", Duration::from_secs(1)).unwrap().contains("states.sensor['1st_floor']"));
        for bad in ["sensor", "sensor.", ".x", "sensor.x }}{{ 1", "Sensor.x", "sensor.x.y"] {
            assert!(matches!(silence_template(bad, Duration::from_secs(1)), Err(Error::InvalidInput(_))), "{bad}");
        }
    }
}
