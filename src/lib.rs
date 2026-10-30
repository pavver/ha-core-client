//! A strongly-typed, resilient asynchronous client for the Home Assistant WebSocket API.
//!
//! ```no_run
//! use ha_core_client::{HAClient, Update};
//!
//! # async fn example() -> ha_core_client::Result<()> {
//! let client = HAClient::connect("ws://homeassistant.local:8123/api/websocket", "TOKEN").await?;
//!
//! client.subscribe_entity("binary_sensor.motion", |client, update| async move {
//!     if let Update::Event(change) = update {
//!         if change.new_state.is_some_and(|state| state.state == "on") {
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
mod source;
pub mod state;
mod stream;
mod subscription;
mod target;
pub mod triggers;

pub use config::ClientConfig;
pub use error::{Error, HaErrorCode, Result};
pub use target::Target;
pub use events::{BusEvent, ServiceCall, Silence, State, StateChange, SubscriptionStatus, TemplateUpdate, TriggerFired, Update};
pub use source::Source;
pub use stream::EventStream;
pub use subscription::{BoxError, CallbackResult, EntityChanges, Subscription, SubscriptionGuard};

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use log::debug;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, watch};

use crate::events::HistoricalState;
use crate::runner::{Runner, Shared};
use crate::state::{Domain, Service};
use crate::source::into_update;
use crate::subscription::{Command, Delivery, EventCallback};

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
/// they receive their own handle while they run. A callback that needs a handle outside of
/// its own run should capture a [`WeakClient`] ([`downgrade`](Self::downgrade)), so that
/// dropping all user handles still stops the client. An [`EventStream`] and a
/// [`SubscriptionGuard`] hold a handle too.
#[derive(Clone)]
pub struct HAClient {
    cmd_tx: mpsc::Sender<Command>,
    shared: Arc<Shared>,
}

/// A handle that does not keep the client running, from [`HAClient::downgrade`].
#[derive(Clone)]
pub struct WeakClient {
    cmd_tx: mpsc::WeakSender<Command>,
    shared: Arc<Shared>,
}

impl WeakClient {
    /// The client, or `None` if every [`HAClient`] handle is gone (the client is stopping).
    pub fn upgrade(&self) -> Option<HAClient> {
        self.cmd_tx.upgrade().map(|cmd_tx| HAClient { cmd_tx, shared: self.shared.clone() })
    }
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
        static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);
        let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
        let shared = Arc::new(Shared { client_id, config, shutdown_tx, state_tx, workers });

        let runner = Runner::new(shared.clone(), cmd_tx.downgrade());
        tokio::spawn(runner.run(ws, cmd_rx, shutdown_rx));

        Ok(Self { cmd_tx, shared })
    }

    /// A handle that does not keep the client running. Capture it in callbacks instead of an
    /// `HAClient`, which would keep the client alive for as long as the subscription exists.
    pub fn downgrade(&self) -> WeakClient {
        WeakClient { cmd_tx: self.cmd_tx.downgrade(), shared: self.shared.clone() }
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

    async fn request(&self, request: protocol::Request) -> Result<Box<RawValue>> {
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
        let Ok(items) = serde_json::from_str::<Vec<Box<RawValue>>>(result.get()) else {
            return Err(Error::Protocol("get_states did not return a list".into()));
        };
        Ok(items
            .into_iter()
            .filter_map(|item| {
                serde_json::from_str::<State>(item.get())
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
        Ok(parse_history(&result, entity_id)?)
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
                code: HaErrorCode::NotFound,
                message: format!("No state for '{entity_id}' in the window ending at {timestamp}"),
            })
    }

    // --- Services ---

    /// Calls a Home Assistant service.
    ///
    /// `domain` and `service` accept typed values or plain strings. `target` is anything that
    /// converts into a [`Target`]: `()` for none, an entity id, a list of entity ids, or a
    /// `Target` built for areas, devices, floors and labels. `data` is any value that serializes
    /// to a JSON object (`()` for no service data); anything else fails with
    /// [`Error::InvalidInput`].
    ///
    /// ```no_run
    /// # use ha_core_client::{HAClient, Target, state::{Domain, Service}};
    /// # use serde_json::json;
    /// # async fn example(client: HAClient) -> ha_core_client::Result<()> {
    /// client.call_service("light", "turn_on", "light.kitchen", json!({"brightness": 120})).await?;
    /// client.call_service(Domain::HomeAssistant, Service::Reload, (), ()).await?;
    /// client.call_service("light", "turn_off", Target::area("kitchen").and_entity("light.porch"), ()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn call_service(
        &self,
        domain: impl Into<Domain>,
        service: impl Into<Service>,
        target: impl Into<Target>,
        data: impl Serialize,
    ) -> Result<()> {
        self.service_call(domain.into(), service.into(), target.into(), data, false).await.map(drop)
    }

    /// Calls a service that returns data (e.g. `weather.get_forecasts`) and decodes its
    /// `response` into `R`. A missing response decodes from `null` (`Value::Null`, or `None`
    /// for an `Option`).
    ///
    /// ```no_run
    /// # use ha_core_client::HAClient;
    /// # use serde_json::{json, Value};
    /// # async fn example(client: HAClient) -> ha_core_client::Result<()> {
    /// let forecast: Value = client
    ///     .call_service_with_response("weather", "get_forecasts", "weather.home", json!({"type": "daily"}))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn call_service_with_response<R: DeserializeOwned>(
        &self,
        domain: impl Into<Domain>,
        service: impl Into<Service>,
        target: impl Into<Target>,
        data: impl Serialize,
    ) -> Result<R> {
        let result = self.service_call(domain.into(), service.into(), target.into(), data, true).await?;
        let wrapper: ServiceResponse = serde_json::from_str(result.get())?;
        Ok(serde_json::from_str(wrapper.response.map_or("null", RawValue::get))?)
    }

    async fn service_call(
        &self,
        domain: Domain,
        service: Service,
        target: Target,
        data: impl Serialize,
        return_response: bool,
    ) -> Result<Box<RawValue>> {
        let service_data = match serde_json::to_value(data) {
            Ok(Value::Null) => None,
            Ok(data @ Value::Object(_)) => Some(data),
            Ok(other) => {
                return Err(Error::InvalidInput(format!("service data must be a JSON object, got {other}")));
            }
            Err(e) => return Err(Error::InvalidInput(format!("service data cannot be serialized: {e}"))),
        };
        debug!("Calling service: {}.{} for target: {:?}", domain, service, target);
        let request = protocol::call_service(domain.as_str(), service.as_str(), &target, service_data, return_response);
        self.request(request).await
    }

    // --- Subscriptions ---

    /// Subscribes to `source` and runs `callback` for each [`Update`]: data events as
    /// [`Update::Event`], changes of the subscription itself (connection lost, restored, …)
    /// as [`Update::Status`]. Callbacks of one subscription run one after another, in order.
    /// The convenience methods (`subscribe_entity`, `subscribe_trigger`, …) are shortcuts.
    ///
    /// While the client is reconnecting, subscribing succeeds right away: the subscription
    /// starts with `SubscriptionStatus::Interrupted` and is sent once the connection is back
    /// (`Restored`, or `Rejected` if Home Assistant refuses it). The existence check of
    /// `subscribe_entity` is skipped in that case.
    ///
    /// A callback keeps whatever it captures alive for as long as the subscription exists.
    /// It receives its own [`HAClient`] while it runs, so it does not need to capture one; if
    /// it must call Home Assistant from elsewhere, capture a [`WeakClient`], otherwise
    /// dropping the last user handle no longer stops the client.
    pub async fn subscribe<T, F, Fut>(&self, source: Source<T>, callback: F) -> Result<Subscription>
    where
        T: Send + 'static,
        F: Fn(HAClient, Update<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        debug!("Subscribing: {}", source.request);
        let Source { kind, request, convert } = source;
        // The conversion runs in the subscription's worker, not in the runner.
        let callback: EventCallback = Arc::new(move |client, event, sub_id| match into_update(convert, event, sub_id) {
            Some(update) => Box::pin(callback(client, update)),
            None => Box::pin(std::future::ready(Ok(()))),
        });
        let delivery = Delivery::Callback(callback);
        let subscribed = self.call(|responder| Command::Subscribe { kind, request, delivery, responder }).await?;
        Ok(subscribed.subscription)
    }

    /// Subscribes to `source` and returns its updates as a [`Stream`](futures_util::Stream),
    /// instead of running a callback. Behaves like [`subscribe`](Self::subscribe) otherwise.
    ///
    /// ```no_run
    /// # use ha_core_client::{HAClient, Source, Update};
    /// # use futures_util::StreamExt;
    /// # async fn example(client: HAClient) -> ha_core_client::Result<()> {
    /// let mut stream = client.stream(Source::entity("sensor.temperature")).await?;
    /// while let Some(update) = stream.next().await {
    ///     if let Update::Event(change) = update {
    ///         println!("{} -> {:?}", change.entity_id, change.new_state.map(|s| s.state));
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The stream holds a handle to the client (so it keeps it running) and unsubscribes when
    /// dropped. See [`EventStream`].
    pub async fn stream<T: Send + 'static>(&self, source: Source<T>) -> Result<EventStream<T>> {
        debug!("Subscribing (stream): {}", source.request);
        let Source { kind, request, convert } = source;
        let subscribed =
            self.call(|responder| Command::Subscribe { kind, request, delivery: Delivery::Stream, responder }).await?;
        let rx = subscribed.rx.ok_or(Error::Stopped)?;
        Ok(EventStream::new(self.clone(), subscribed.subscription, rx, convert))
    }

    /// Subscribes to changes of one entity (state and attributes).
    ///
    /// The callback receives [`StateChange`]s. Filtering happens in Home Assistant,
    /// so only this entity's changes are transferred. Callbacks of one subscription run
    /// one after another, in order.
    ///
    /// Fails with [`Error::Ha`] (code `not_found`) if the entity does not exist, which
    /// catches typos. To wait for an entity that does not exist yet, use
    /// [`subscribe_trigger`](Self::subscribe_trigger) with a [`StateTrigger`](triggers::StateTrigger) instead.
    ///
    /// After a reconnect the entity's state is re-read: if it changed while disconnected,
    /// the callback gets one `StateChange` from the last known to the current state,
    /// followed by [`Update::Status`] with [`SubscriptionStatus::Restored`].
    ///
    /// Same as [`subscribe_entity_with`](Self::subscribe_entity_with) with [`EntityChanges::Any`].
    pub async fn subscribe_entity<F, Fut>(&self, entity_id: &str, callback: F) -> Result<Subscription>
    where
        F: Fn(HAClient, Update<StateChange>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        self.subscribe(Source::entity(entity_id), callback).await
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
        F: Fn(HAClient, Update<StateChange>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        self.subscribe(Source::entity_with(entity_id, changes), callback).await
    }

    /// Subscribes to every state change in the system (the `state_changed` bus event).
    /// Prefer [`subscribe_entity`](Self::subscribe_entity) for single entities: this one
    /// transfers all of them, and has no existence check, resync after reconnects or merging.
    pub async fn subscribe_state_changes<F, Fut>(&self, callback: F) -> Result<Subscription>
    where
        F: Fn(HAClient, Update<StateChange>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        self.subscribe(Source::state_changes(), callback).await
    }

    /// Subscribes to bus events of one type (custom events, …), delivered as [`BusEvent`]s.
    ///
    /// `state_changed` and `call_service` are not special-cased here; use
    /// [`subscribe_state_changes`](Self::subscribe_state_changes) and
    /// [`subscribe_service_calls`](Self::subscribe_service_calls) for typed events.
    pub async fn subscribe_events<F, Fut>(&self, event_type: &str, callback: F) -> Result<Subscription>
    where
        F: Fn(HAClient, Update<BusEvent>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        self.subscribe(Source::events(event_type), callback).await
    }

    /// Subscribes to all `call_service` events on the Home Assistant event bus.
    pub async fn subscribe_service_calls<F, Fut>(&self, callback: F) -> Result<Subscription>
    where
        F: Fn(HAClient, Update<ServiceCall>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        self.subscribe(Source::service_calls(), callback).await
    }

    /// Subscribes to a Home Assistant trigger. The callback receives [`TriggerFired`]s.
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
        F: Fn(HAClient, Update<TriggerFired>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        self.subscribe(Source::trigger(trigger), callback).await
    }

    /// Subscribes to several triggers at once; any of them fires the callback. Give each
    /// platform an `.id(..)` and read [`TriggerFired::id`] to tell them apart.
    ///
    /// Fails with [`Error::Ha`] if Home Assistant rejects a trigger configuration.
    pub async fn subscribe_triggers<I, T, F, Fut>(&self, triggers: I, callback: F) -> Result<Subscription>
    where
        I: IntoIterator<Item = T>,
        T: Into<triggers::Trigger>,
        F: Fn(HAClient, Update<TriggerFired>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        self.subscribe(Source::triggers(triggers), callback).await
    }

    /// Subscribes to a trigger given as raw JSON, for trigger platforms and options
    /// not modelled by [`triggers::Trigger`].
    ///
    /// Fails with [`Error::InvalidInput`] if `trigger` is not a JSON object.
    pub async fn subscribe_trigger_raw<F, Fut>(&self, trigger: Value, callback: F) -> Result<Subscription>
    where
        F: Fn(HAClient, Update<TriggerFired>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        self.subscribe(Source::trigger_raw(trigger)?, callback).await
    }

    /// Subscribes to a template rendered by Home Assistant. The callback receives
    /// [`TemplateUpdate::Rendered`] with the current result right after subscribing, then
    /// each time the result changes, and [`TemplateUpdate::RenderError`] if a later render fails.
    ///
    /// Home Assistant re-renders when a referenced entity changes, and at the start of
    /// every minute if the template uses `now()`. Results are parsed like in automations
    /// (`"{{ 1 + 1 }}"` gives the number `2`). A syntax error fails the subscription with
    /// [`Error::Ha`]; a render that fails (`{{ 1 / 0 }}`) arrives as `RenderError`.
    ///
    /// After a reconnect the template is rendered again, so the current result follows
    /// [`SubscriptionStatus::Restored`], also if it did not change.
    pub async fn subscribe_template<F, Fut>(&self, template: &str, callback: F) -> Result<Subscription>
    where
        F: Fn(HAClient, Update<TemplateUpdate>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        self.subscribe(Source::template(template), callback).await
    }

    /// Like [`subscribe_template`](Self::subscribe_template), decoding each result into `T`
    /// (`f64`, `bool`, `String`, a struct, …). A result that does not fit arrives as
    /// [`TemplateUpdate::DecodeError`].
    pub async fn subscribe_template_as<T, F, Fut>(&self, template: &str, callback: F) -> Result<Subscription>
    where
        T: DeserializeOwned + Send + 'static,
        F: Fn(HAClient, Update<TemplateUpdate<T>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        self.subscribe(Source::template_as::<T>(template), callback).await
    }

    /// Watches whether an entity keeps reporting: the callback receives [`Silence`]
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
        F: Fn(HAClient, Update<Silence>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallbackResult> + Send + 'static,
    {
        self.subscribe(Source::silence(entity_id, max_silence)?, callback).await
    }

    /// Cancels a subscription. Events already queued for its callback are still delivered,
    /// followed by [`SubscriptionStatus::Unsubscribed`].
    ///
    /// Fails with [`Error::InvalidInput`] for a subscription of another client.
    pub async fn unsubscribe(&self, subscription: Subscription) -> Result<()> {
        debug!("Unsubscribing from subscription ID: {}", subscription.id);
        self.check_owner(&subscription)?;
        let id = subscription.id;
        self.call(|responder| Command::Unsubscribe { id, responder }).await.map(drop)
    }

    /// Unsubscribes without waiting, for `SubscriptionGuard::drop`.
    pub(crate) fn unsubscribe_detached(&self, subscription: Subscription) {
        if let Err(e) = self.check_owner(&subscription) {
            log::warn!("Not unsubscribing: {e}");
            return;
        }
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

    /// Ids are per client, so another client's subscription would cancel an unrelated one.
    fn check_owner(&self, subscription: &Subscription) -> Result<()> {
        if subscription.client == self.shared.client_id {
            Ok(())
        } else {
            Err(Error::InvalidInput(format!("subscription {} belongs to another client", subscription.id)))
        }
    }
}

#[derive(Deserialize)]
struct ServiceResponse<'a> {
    #[serde(borrow, default)]
    response: Option<&'a RawValue>,
}

/// Parses the `history_during_period` result for `entity_id`, skipping entries that do
/// not parse, so one bad record does not lose the whole history.
fn parse_history(result: &RawValue, entity_id: &str) -> serde_json::Result<Vec<HistoricalState>> {
    let mut by_entity: HashMap<String, Vec<Box<RawValue>>> = serde_json::from_str(result.get())?;
    Ok(by_entity
        .remove(entity_id)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|item| {
            serde_json::from_str::<events::state::HistoricalStateWire>(item.get())
                .inspect_err(|e| log::warn!("Skipping unparsable history entry of {entity_id}: {e}"))
                .ok()
        })
        .map(|wire| wire.into_state(entity_id))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_skips_bad_entries() {
        let result = serde_json::value::to_raw_value(&serde_json::json!({"sensor.t": [
            {"s": "1", "a": {}, "lu": 100.0},
            {"s": "2", "lu": "not a timestamp"},
            {"s": "3", "lu": 300.0}
        ]})).unwrap();
        let states: Vec<_> = parse_history(&result, "sensor.t").unwrap().into_iter().map(|h| h.state).collect();
        assert_eq!(states, ["1", "3"]);
        assert!(parse_history(&serde_json::value::to_raw_value(&serde_json::json!({})).unwrap(), "sensor.t").unwrap().is_empty());
    }
}
