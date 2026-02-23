pub mod connection;
pub mod context_helpers;
pub mod events;
pub mod pending;
pub mod handler;
pub mod protocol;
pub mod runner;
pub mod state;
pub mod subscription;
pub mod triggers;

use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::atomic::AtomicU64; 
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use log::{info, debug};
use std::time::Duration;

use crate::events::HistoricalState;
use crate::connection::WsConnection;
use crate::state::{Domain, HaState, Service};
use crate::subscription::{
    EventCallback, EventFilter, HandlerCommand, Subscription,
};


/// The main handle for interacting with the Home Assistant WebSocket API.
/// 
/// `HAClient` follows the handle/runner pattern. It is a lightweight, cloneable 
/// handle that sends commands to a background runner task.
pub struct HAClient {
    command_tx: Option<Arc<mpsc::Sender<HandlerCommand>>>,
    shutdown_tx: Option<Arc<watch::Sender<bool>>>,
    #[doc(hidden)]
    task_handle: Option<JoinHandle<Result<()>>>,
}

impl Clone for HAClient {
    fn clone(&self) -> Self {
        Self {
            command_tx: self.command_tx.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
            task_handle: None, 
        }
    }
}

impl Default for HAClient {
    fn default() -> Self {
        Self::new()
    }
}

impl HAClient {
    /// Creates a new, un-connected Home Assistant client.
    pub fn new() -> Self {
        debug!("Creating new HAClient.");
        Self {
            command_tx: None,
            shutdown_tx: None,
            task_handle: None,
        }
    }

    /// Connects to the Home Assistant instance, authenticates, and starts the background event loop.
    /// 
    /// # Arguments
    /// * `url` - The WebSocket URL of the Home Assistant instance (e.g., "ws://localhost:8123/api/websocket").
    /// * `token` - A Long-Lived Access Token generated in Home Assistant.
    pub async fn run(&mut self, url: &str, token: &str) -> Result<()> {
        info!("HAClient attempting to run with URL: {} and token (hidden).", url);
        if self.task_handle.is_some() {
            return Err(anyhow!("Client is already running."));
        }

        let conn = WsConnection::new(url, token).await?;
        let (command_tx_inner, command_rx) = mpsc::channel(100);
        let command_tx = Arc::new(command_tx_inner);
        let (shutdown_tx_inner, shutdown_rx) = watch::channel(false);
        let shutdown_tx = Arc::new(shutdown_tx_inner);

        self.command_tx = Some(command_tx.clone());
        self.shutdown_tx = Some(shutdown_tx.clone());

        let runner = ClientRunner {
            conn,
            msg_id: AtomicU64::new(1),
            subscriptions: HashMap::new(),
            pending_one_shot_gets: HashMap::new(),
            pending_api_calls: HashMap::new(),
            shutdown_tx: shutdown_tx.clone(),
        };

        let handle = tokio::spawn(runner.run_loop(command_rx, shutdown_rx, command_tx.clone()));
        self.task_handle = Some(handle);
        debug!("HAClient runner task spawned.");

        Ok(())
    }

    /// Stops the client's background event loop and waits for it to finish.
    pub async fn stop(&mut self) -> Result<()> {
        info!("HAClient stopping...");
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            if shutdown_tx.send(true).is_err() {
                return Err(anyhow!(
                    "Failed to send shutdown signal: receiver was dropped."
                ));
            }
            debug!("Shutdown signal sent to HAClient runner.");
        }

        if let Some(handle) = self.task_handle.take() {
            match handle.await {
                Ok(Ok(_)) => {
                    info!("HAClient stopped successfully.");
                    Ok(())
                },
                Ok(Err(e)) => Err(e),
                Err(e) => Err(anyhow!("Client task panicked: {}", e)),
            }
        } else {
            Ok(())
        }
    }

    fn get_command_tx(&self) -> Result<&Arc<mpsc::Sender<HandlerCommand>>> {
        self.command_tx
            .as_ref()
            .ok_or_else(|| anyhow!("Client is not running."))
    }

    /// Subscribes to `state_changed` events for a specific entity.
    /// 
    /// The provided callback will be invoked whenever the entity's state changes.
    pub async fn subscribe_entity<F, Fut>(
        &self,
        entity_id: &str,
        callback: F,
    ) -> Result<Subscription>
    where
        F: Fn(crate::HAClient, crate::events::Event) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        debug!("Subscribing to entity: {}", entity_id);
        let (tx, rx) = oneshot::channel();
        let cmd = HandlerCommand::Subscribe {
            details: crate::subscription::InternalSubscriptionDetails::Event {
                event_type: "state_changed".to_string(),
                filter: EventFilter::ByEntityId(entity_id.to_string()),
            },
            callback: Arc::new(move |client, event| Box::pin(callback(client, event))), 
            responder: tx,
        };
        self.get_command_tx()?.send(cmd).await?;
        rx.await?
    }

    /// Unsubscribes from an active event stream or trigger.
    pub async fn unsubscribe(&self, subscription: Subscription) -> Result<()> {
        debug!("Unsubscribing from subscription ID: {}", subscription.id);
        let (tx, rx) = oneshot::channel();
        let cmd = HandlerCommand::Unsubscribe {
            subscription_id: subscription.id,
            responder: tx,
        };
        self.get_command_tx()?.send(cmd).await?;
        rx.await?
    }

    /// Fetches the current state of a specific entity.
    pub async fn get_state(&self, entity_id: &str) -> Result<events::State> {
        debug!("Requesting state for entity: {}", entity_id);
        let (tx, rx) = oneshot::channel();
        let cmd = HandlerCommand::GetStateOnce {
            entity_id: entity_id.to_string(),
            responder: tx,
        };
        self.get_command_tx()?.send(cmd).await?;
        rx.await?
    }

    /// Fetches the history of states for a specific entity within a time range.
    pub async fn get_history(
        &self,
        entity_id: &str,
        start_time: chrono::DateTime<chrono::Utc>,
        end_time: chrono::DateTime<chrono::Utc>,
        significant_changes_only: Option<bool>,
    ) -> Result<Vec<HistoricalState>> {
        let (tx, rx) = oneshot::channel();
        let cmd = HandlerCommand::GetHistory {
            entity_id: entity_id.to_string(),
            start_time,
            end_time,
            significant_changes_only,
            responder: tx,
        };
        self.get_command_tx()?.send(cmd).await?;
        rx.await?
    }

    /// Finds the state of an entity at a specific point in time in the past.
    /// 
    /// This method queries the history API within a `lookback_duration` window 
    /// ending at the target `timestamp` and returns the latest state recorded 
    /// at or before that timestamp.
    pub async fn get_state_at_time(
        &self,
        entity_id: &str,
        timestamp: chrono::DateTime<chrono::Utc>,
        lookback_duration: chrono::Duration,
    ) -> Result<events::State> {
        debug!(
            "Requesting state for entity: {} at time: {} with lookback duration: {:?}",
            entity_id, timestamp, lookback_duration
        );
        let start_time = timestamp - lookback_duration;
        let history = self
            .get_history(entity_id, start_time, timestamp, Some(true))
            .await?;

        get_state_from_history(history, timestamp).ok_or_else(|| {
            anyhow!(
                "Could not find a state for entity '{}' in the time window ending at {}",
                entity_id,
                timestamp
            )
        })
    }

    /// Fetches the current state of all entities in Home Assistant.
    pub async fn get_states(&self) -> Result<Vec<events::State>> {
        debug!("Requesting all states");
        let (tx, rx) = oneshot::channel();
        let cmd = HandlerCommand::GetStates { responder: tx };
        self.get_command_tx()?.send(cmd).await?;
        rx.await?
    }

    /// Subscribes to all `call_service` events on the Home Assistant event bus.
    pub async fn subscribe_service_calls<F, Fut>(
        &self,
        callback: F,
    ) -> Result<Subscription>
    where
        F: Fn(crate::HAClient, crate::events::Event) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        debug!("Subscribing to all service_called events");
        let (tx, rx) = oneshot::channel();
        let cmd = HandlerCommand::Subscribe {
            details: crate::subscription::InternalSubscriptionDetails::Event {
                event_type: "call_service".to_string(),
                filter: EventFilter::Any,
            },
            callback: Arc::new(move |client, event| Box::pin(callback(client, event))), 
            responder: tx,
        };
        self.get_command_tx()?.send(cmd).await?;
        rx.await?
    }

    /// Generic method to call any Home Assistant service.
    pub async fn call_service(
        &self,
        domain: Domain,
        service: Service,
        entity_id: Option<&str>,
        service_data: Option<serde_json::Value>,
    ) -> Result<()> {
        debug!("Calling service: {}.{} for entity: {:?}", domain, service, entity_id);
        let (tx, rx) = oneshot::channel();
        let cmd = HandlerCommand::CallService {
            domain,
            service,
            entity_id: entity_id.map(|s| s.to_string()),
            service_data,
            responder: tx,
        };
        self.get_command_tx()?.send(cmd).await?;
        rx.await?
    }

    async fn subscribe_trigger_internal(
        &self,
        trigger_config: triggers::Trigger,
        callback: EventCallback,
    ) -> Result<Subscription> {
        debug!("Subscribing to trigger: {:?}", trigger_config);
        let (tx, rx) = oneshot::channel();
        let cmd = HandlerCommand::Subscribe {
            details: crate::subscription::InternalSubscriptionDetails::Trigger { trigger: trigger_config },
            callback,
            responder: tx,
        };
        self.get_command_tx()?.send(cmd).await?;
        rx.await?
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
        F: Fn(crate::HAClient, crate::events::Event) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let trigger_config = triggers::Trigger::State(triggers::StateTrigger {
            entity_id: entity_id.to_string(),
            to,
            from,
            r#for: for_duration,
            attribute: None,
        });
        let arc_callback: EventCallback = Arc::new(move |client, event| Box::pin(callback(client, event)));
        self.subscribe_trigger_internal(trigger_config, arc_callback).await
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
        F: Fn(crate::HAClient, crate::events::Event) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let trigger_config = triggers::Trigger::NumericState(triggers::NumericStateTrigger {
            entity_id: entity_id.to_string(),
            above,
            below,
            r#for: for_duration,
            attribute: None,
        });
        let arc_callback: EventCallback = Arc::new(move |client, event| Box::pin(callback(client, event)));
        self.subscribe_trigger_internal(trigger_config, arc_callback).await
    }

    /// Subscribes to a sun trigger (sunrise or sunset).
    pub async fn subscribe_sun_trigger<F, Fut>(
        &self,
        event: triggers::SunEvent,
        offset: Option<Duration>,
        callback: F,
    ) -> Result<Subscription>
    where
        F: Fn(crate::HAClient, crate::events::Event) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let trigger_config = triggers::Trigger::Sun(triggers::SunTrigger {
            event,
            offset,
        });
        let arc_callback: EventCallback = Arc::new(move |client, event| Box::pin(callback(client, event)));
        self.subscribe_trigger_internal(trigger_config, arc_callback).await
    }

    /// Subscribes to a time trigger.
    /// 
    /// `at` is a time string like "12:00:00".
    pub async fn subscribe_time_trigger<F, Fut>(
        &self,
        at: String,
        callback: F,
    ) -> Result<Subscription>
    where
        F: Fn(crate::HAClient, crate::events::Event) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let trigger_config = triggers::Trigger::Time(triggers::TimeTrigger { at });
        let arc_callback: EventCallback = Arc::new(move |client, event| Box::pin(callback(client, event)));
        self.subscribe_trigger_internal(trigger_config, arc_callback).await
    }

    /// Subscribes to a time pattern trigger (cron-like).
    pub async fn subscribe_time_pattern_trigger<F, Fut>(
        &self,
        hours: Option<String>,
        minutes: Option<String>,
        seconds: Option<String>,
        callback: F,
    ) -> Result<Subscription>
    where
        F: Fn(crate::HAClient, crate::events::Event) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let trigger_config = triggers::Trigger::TimePattern(triggers::TimePatternTrigger {
            hours,
            minutes,
            seconds,
        });
        let arc_callback: EventCallback = Arc::new(move |client, event| Box::pin(callback(client, event)));
        self.subscribe_trigger_internal(trigger_config, arc_callback).await
    }

    /// Subscribes to a template trigger.
    /// 
    /// Evaluates a Home Assistant Jinja2 template and fires when it turns true.
    pub async fn subscribe_template_trigger<F, Fut>(
        &self,
        value_template: String,
        for_duration: Option<Duration>,
        callback: F,
    ) -> Result<Subscription>
    where
        F: Fn(crate::HAClient, crate::events::Event) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let trigger_config = triggers::Trigger::Template(triggers::TemplateTrigger {
            value_template,
            r#for: for_duration,
        });
        let arc_callback: EventCallback = Arc::new(move |client, event| Box::pin(callback(client, event)));
        self.subscribe_trigger_internal(trigger_config, arc_callback).await
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
        F: Fn(crate::HAClient, crate::events::Event) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let trigger_config = triggers::Trigger::Zone(triggers::ZoneTrigger {
            entity_id: entity_id.to_string(),
            zone: zone.to_string(),
            event,
        });
        let arc_callback: EventCallback = Arc::new(move |client, event| Box::pin(callback(client, event)));
        self.subscribe_trigger_internal(trigger_config, arc_callback).await
    }
}

use crate::runner::ClientRunner;

// --- Private helper functions ---

fn get_state_from_history(
    history: Vec<HistoricalState>,
    target_timestamp: chrono::DateTime<chrono::Utc>,
) -> Option<events::State> {
    history
        .into_iter()
        .filter(|state| state.last_updated <= target_timestamp)
        .max_by_key(|state| state.last_updated)
        .map(Into::into)
}