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
mod runner;
pub mod state;
mod subscription;
pub mod triggers;

pub use config::ClientConfig;
pub use error::{Error, Result};
pub use subscription::Subscription;

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
use crate::state::{Domain, HaState, Service};
use crate::subscription::{boxed_callback, Command, SubKind};

/// Connection status of an [`HAClient`], see [`HAClient::watch_connection_state`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionState {
    /// Connected and authenticated; subscriptions are active.
    Connected,
    /// The connection was lost; requests fail with [`Error::NotConnected`] until it is back.
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
    /// automatically.
    pub async fn connect(url: impl Into<String>, token: impl Into<String>) -> Result<Self> {
        Self::connect_with(ClientConfig::new(url, token)).await
    }

    /// Connects and authenticates with custom settings. See [`connect`](Self::connect).
    pub async fn connect_with(config: ClientConfig) -> Result<Self> {
        let ws = connection::connect_and_auth(&config).await?;

        let (cmd_tx, cmd_rx) = mpsc::channel(256);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (state_tx, _) = watch::channel(ConnectionState::Connected);
        let shared = Arc::new(Shared { config, shutdown_tx, state_tx });

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
    pub async fn stop(&self) {
        debug!("Stopping HAClient...");
        self.shared.shutdown_tx.send_replace(true);
        self.closed().await;
    }

    async fn call<T>(&self, make: impl FnOnce(oneshot::Sender<Result<T>>) -> Command) -> Result<T> {
        if self.connection_state().is_terminal() {
            return Err(Error::Stopped);
        }
        let (tx, rx) = oneshot::channel();
        self.cmd_tx.send(make(tx)).await.map_err(|_| Error::Stopped)?;
        // A little slack so that the runner's own expiry usually answers first.
        let timeout = self.shared.config.request_timeout + Duration::from_secs(1);
        match tokio::time::timeout(timeout, rx).await {
            Err(_) => Err(Error::Timeout),
            Ok(Err(_)) => Err(Error::Stopped),
            Ok(Ok(result)) => result,
        }
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
    pub async fn get_history(
        &self,
        entity_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        significant_changes_only: bool,
    ) -> Result<Vec<HistoricalState>> {
        let request = protocol::history_during_period(entity_id, start_time, end_time, significant_changes_only);
        let result = self.request(request).await?;
        let mut by_entity: HashMap<String, Vec<events::state::HistoricalStateWire>> = serde_json::from_value(result)?;
        Ok(by_entity
            .remove(entity_id)
            .unwrap_or_default()
            .into_iter()
            .map(|wire| wire.into_state(entity_id))
            .collect())
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

    async fn subscribe_with<F, Fut>(&self, kind: SubKind, request: protocol::Request, callback: F) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let callback = boxed_callback(callback);
        self.call(|responder| Command::Subscribe { kind, request, callback, responder }).await
    }

    /// Subscribes to changes of one entity (state and attributes).
    ///
    /// The callback receives [`Event::StateChanged`]. Filtering happens in Home Assistant,
    /// so only this entity's changes are transferred. Callbacks of one subscription run
    /// one after another, in order.
    pub async fn subscribe_entity<F, Fut>(&self, entity_id: &str, callback: F) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        debug!("Subscribing to entity: {}", entity_id);
        let trigger = serde_json::json!({ "platform": "state", "entity_id": entity_id });
        self.subscribe_with(SubKind::EntityState, protocol::subscribe_trigger(&trigger), callback).await
    }

    /// Subscribes to bus events of one type (`state_changed`, `call_service`, custom events, …).
    ///
    /// Note that `state_changed` delivers every state change in the system; prefer
    /// [`subscribe_entity`](Self::subscribe_entity) for single entities.
    pub async fn subscribe_events<F, Fut>(&self, event_type: &str, callback: F) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        debug!("Subscribing to events: {}", event_type);
        self.subscribe_with(SubKind::Events, protocol::subscribe_events(event_type), callback).await
    }

    /// Subscribes to all `call_service` events on the Home Assistant event bus.
    pub async fn subscribe_service_calls<F, Fut>(&self, callback: F) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.subscribe_events("call_service", callback).await
    }

    /// Subscribes to a Home Assistant trigger. The callback receives [`Event::TriggerFired`].
    ///
    /// Fails with [`Error::Ha`] if Home Assistant rejects the trigger configuration.
    pub async fn subscribe_trigger<F, Fut>(&self, trigger: triggers::Trigger, callback: F) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        debug!("Subscribing to trigger: {:?}", trigger);
        let trigger = serde_json::to_value(trigger)?;
        self.subscribe_trigger_raw(trigger, callback).await
    }

    /// Subscribes to a trigger given as raw JSON, for trigger platforms and options
    /// not modelled by [`triggers::Trigger`].
    pub async fn subscribe_trigger_raw<F, Fut>(&self, trigger: Value, callback: F) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.subscribe_with(SubKind::Trigger, protocol::subscribe_trigger(&trigger), callback).await
    }

    /// Cancels a subscription. Events already queued for its callback are still delivered.
    pub async fn unsubscribe(&self, subscription: Subscription) -> Result<()> {
        debug!("Unsubscribing from subscription ID: {}", subscription.id);
        let id = subscription.id;
        self.call(|responder| Command::Unsubscribe { id, responder }).await.map(drop)
    }

    /// Subscribes to a state trigger.
    ///
    /// Fires when an entity changes state.
    pub async fn subscribe_state_trigger<F, Fut>(
        &self,
        entity_id: &str,
        to: Option<HaState>,
        from: Option<HaState>,
        for_duration: Option<Duration>,
        callback: F,
    ) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let trigger = triggers::Trigger::State(triggers::StateTrigger {
            entity_id: entity_id.to_string(),
            to,
            from,
            r#for: for_duration,
            attribute: None,
        });
        self.subscribe_trigger(trigger, callback).await
    }

    /// Subscribes to a numeric state trigger.
    ///
    /// Fires when an entity's numeric state crosses a threshold.
    pub async fn subscribe_numeric_state_trigger<F, Fut>(
        &self,
        entity_id: &str,
        above: Option<f64>,
        below: Option<f64>,
        for_duration: Option<Duration>,
        callback: F,
    ) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let trigger = triggers::Trigger::NumericState(triggers::NumericStateTrigger {
            entity_id: entity_id.to_string(),
            above,
            below,
            r#for: for_duration,
            attribute: None,
        });
        self.subscribe_trigger(trigger, callback).await
    }

    /// Subscribes to a sun trigger (sunrise or sunset). A negative `offset` fires before the event.
    pub async fn subscribe_sun_trigger<F, Fut>(
        &self,
        event: triggers::SunEvent,
        offset: Option<TimeDelta>,
        callback: F,
    ) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let trigger = triggers::Trigger::Sun(triggers::SunTrigger { event, offset });
        self.subscribe_trigger(trigger, callback).await
    }

    /// Subscribes to a time trigger.
    ///
    /// `at` is a time string like "12:00:00".
    pub async fn subscribe_time_trigger<F, Fut>(&self, at: impl Into<String>, callback: F) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let trigger = triggers::Trigger::Time(triggers::TimeTrigger { at: at.into() });
        self.subscribe_trigger(trigger, callback).await
    }

    /// Subscribes to a time pattern trigger (cron-like), e.g. `minutes: Some("/5")`.
    pub async fn subscribe_time_pattern_trigger<F, Fut>(
        &self,
        hours: Option<String>,
        minutes: Option<String>,
        seconds: Option<String>,
        callback: F,
    ) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let trigger = triggers::Trigger::TimePattern(triggers::TimePatternTrigger { hours, minutes, seconds });
        self.subscribe_trigger(trigger, callback).await
    }

    /// Subscribes to a template trigger.
    ///
    /// Evaluates a Home Assistant Jinja2 template and fires when it turns true.
    pub async fn subscribe_template_trigger<F, Fut>(
        &self,
        value_template: impl Into<String>,
        for_duration: Option<Duration>,
        callback: F,
    ) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let trigger = triggers::Trigger::Template(triggers::TemplateTrigger {
            value_template: value_template.into(),
            r#for: for_duration,
        });
        self.subscribe_trigger(trigger, callback).await
    }

    /// Subscribes to a zone trigger.
    ///
    /// Fires when an entity enters or leaves a zone.
    pub async fn subscribe_zone_trigger<F, Fut>(
        &self,
        entity_id: &str,
        zone: &str,
        event: triggers::ZoneEvent,
        callback: F,
    ) -> Result<Subscription>
    where
        F: Fn(HAClient, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let trigger = triggers::Trigger::Zone(triggers::ZoneTrigger {
            entity_id: entity_id.to_string(),
            zone: zone.to_string(),
            event,
        });
        self.subscribe_trigger(trigger, callback).await
    }
}
