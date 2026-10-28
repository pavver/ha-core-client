use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_tungstenite::tungstenite::Message;

use crate::config::ClientConfig;
use crate::connection::{connect_and_auth, WsStream};
use crate::error::{Error, Result};
use crate::events::{Event, State};
use crate::protocol::{self, Incoming, Request};
use crate::subscription::{Command, EventCallback, SubKind, Subscription};
use crate::{ConnectionState, HAClient};

/// State shared between the runner and every [`HAClient`] handle.
pub(crate) struct Shared {
    pub(crate) config: ClientConfig,
    pub(crate) shutdown_tx: watch::Sender<bool>,
    pub(crate) state_tx: watch::Sender<ConnectionState>,
}

struct SubEntry {
    kind: SubKind,
    request: Request,
    /// Feeds the subscription's worker task, which runs callbacks one at a time.
    events: mpsc::UnboundedSender<(HAClient, Event)>,
    /// Message id of the subscription on the current connection, once acknowledged.
    ha_id: Option<u64>,
}

enum PendingKind {
    Request(oneshot::Sender<Result<Value>>),
    /// `responder` is `None` for re-subscriptions after a reconnect.
    Subscribe {
        sub_id: u64,
        responder: Option<oneshot::Sender<Result<Subscription>>>,
    },
    /// `subscribe_entities` used to read one state; answered by the first event.
    StateOnce {
        entity_id: String,
        responder: oneshot::Sender<Result<State>>,
    },
    /// Fire-and-forget (internal unsubscribes).
    Ignore,
}

struct Pending {
    kind: PendingKind,
    deadline: Instant,
}

enum SessionEnd {
    Stop,
    Lost(String),
}

/// Owns the WebSocket connection and all client state. Runs as a single task,
/// so no locking is needed.
pub(crate) struct Runner {
    shared: Arc<Shared>,
    /// Weak so that dropping every user handle shuts the runner down.
    weak_tx: mpsc::WeakSender<Command>,
    subs: BTreeMap<u64, SubEntry>,
    next_sub_id: u64,

    // Per-connection state; reset by `end_session`.
    next_msg_id: u64,
    pending: HashMap<u64, Pending>,
    routes: HashMap<u64, u64>,
    /// Ids of unwanted server-side subscriptions we already sent an unsubscribe for.
    orphans: HashSet<u64>,
    last_rx: Instant,
    ping: Option<(u64, Instant)>,
}

impl Runner {
    pub(crate) fn new(shared: Arc<Shared>, weak_tx: mpsc::WeakSender<Command>) -> Self {
        Self {
            shared,
            weak_tx,
            subs: BTreeMap::new(),
            next_sub_id: 1,
            next_msg_id: 1,
            pending: HashMap::new(),
            routes: HashMap::new(),
            orphans: HashSet::new(),
            last_rx: Instant::now(),
            ping: None,
        }
    }

    pub(crate) async fn run(
        mut self,
        mut ws: WsStream,
        mut cmd_rx: mpsc::Receiver<Command>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let mut attempt = 0u32;
        loop {
            let end = self.session(&mut ws, &mut cmd_rx, &mut shutdown_rx).await;
            self.end_session();
            match end {
                SessionEnd::Stop => {
                    let _ = tokio::time::timeout(Duration::from_secs(2), ws.close(None)).await;
                    break;
                }
                SessionEnd::Lost(reason) => log::warn!("Connection to Home Assistant lost: {reason}"),
            }

            loop {
                attempt += 1;
                self.set_state(ConnectionState::Reconnecting { attempt });
                let delay = backoff(&self.shared.config, attempt);
                log::info!("Reconnecting in {delay:?} (attempt {attempt})...");
                if self.offline(tokio::time::sleep(delay), &mut cmd_rx, &mut shutdown_rx).await.is_none() {
                    return self.finish(ConnectionState::Stopped);
                }

                let shared = self.shared.clone();
                let connect = async move { connect_and_auth(&shared.config).await };
                match self.offline(connect, &mut cmd_rx, &mut shutdown_rx).await {
                    None => return self.finish(ConnectionState::Stopped),
                    Some(Ok(new_ws)) => {
                        ws = new_ws;
                        attempt = 0;
                        break;
                    }
                    Some(Err(Error::Auth(message))) => {
                        // Retrying a rejected token only risks an IP ban.
                        log::error!("Home Assistant rejected the access token: {message}. Giving up.");
                        return self.finish(ConnectionState::AuthFailed(message));
                    }
                    Some(Err(e)) => log::warn!("Reconnect attempt {attempt} failed: {e}"),
                }
            }
        }
        self.finish(ConnectionState::Stopped);
    }

    fn finish(&mut self, state: ConnectionState) {
        // Dropping the entries ends their worker tasks once queued events are handled.
        self.subs.clear();
        self.set_state(state);
        log::info!("Home Assistant client stopped.");
    }

    fn set_state(&self, state: ConnectionState) {
        self.shared.state_tx.send_replace(state);
    }

    fn client(&self) -> Option<HAClient> {
        Some(HAClient {
            cmd_tx: self.weak_tx.upgrade()?,
            shared: self.shared.clone(),
        })
    }

    // --- Disconnected phase ---

    /// Drives `fut` while answering commands with `NotConnected`.
    /// Returns `None` on shutdown or when all handles are dropped.
    async fn offline<F: Future>(
        &mut self,
        fut: F,
        cmd_rx: &mut mpsc::Receiver<Command>,
        shutdown_rx: &mut watch::Receiver<bool>,
    ) -> Option<F::Output> {
        tokio::pin!(fut);
        loop {
            tokio::select! {
                biased;
                _ = stopped(shutdown_rx) => return None,
                cmd = cmd_rx.recv() => match cmd {
                    None => return None,
                    Some(cmd) => self.handle_offline_command(cmd),
                },
                out = &mut fut => return Some(out),
            }
        }
    }

    fn handle_offline_command(&mut self, cmd: Command) {
        match cmd {
            Command::Request { responder, .. } => drop(responder.send(Err(Error::NotConnected))),
            Command::GetStateOnce { responder, .. } => drop(responder.send(Err(Error::NotConnected))),
            Command::Subscribe { responder, .. } => drop(responder.send(Err(Error::NotConnected))),
            Command::Unsubscribe { id, responder } => {
                self.subs.remove(&id);
                let _ = responder.send(Ok(Value::Null));
            }
        }
    }

    // --- Connected phase ---

    async fn session(
        &mut self,
        ws: &mut WsStream,
        cmd_rx: &mut mpsc::Receiver<Command>,
        shutdown_rx: &mut watch::Receiver<bool>,
    ) -> SessionEnd {
        self.next_msg_id = 1;
        self.last_rx = Instant::now();
        self.ping = None;

        if let Err(reason) = self.resubscribe(ws).await {
            return SessionEnd::Lost(reason);
        }
        self.set_state(ConnectionState::Connected);

        let mut tick = tokio::time::interval(tick_period(&self.shared.config));
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            let step = tokio::select! {
                biased;
                _ = stopped(shutdown_rx) => return SessionEnd::Stop,
                msg = ws.next() => match msg {
                    Some(Ok(Message::Text(text))) => {
                        self.last_rx = Instant::now();
                        self.handle_text(ws, &text).await
                    }
                    Some(Ok(Message::Close(frame))) => {
                        return SessionEnd::Lost(format!("server closed the connection ({frame:?})"));
                    }
                    Some(Ok(_)) => {
                        self.last_rx = Instant::now();
                        Ok(())
                    }
                    Some(Err(e)) => return SessionEnd::Lost(e.to_string()),
                    None => return SessionEnd::Lost("connection closed".into()),
                },
                cmd = cmd_rx.recv() => match cmd {
                    None => return SessionEnd::Stop,
                    Some(cmd) => self.handle_command(ws, cmd).await,
                },
                _ = tick.tick() => self.housekeeping(ws).await,
            };
            if let Err(reason) = step {
                return SessionEnd::Lost(reason);
            }
        }
    }

    /// Fails everything that was in flight on the connection that just ended.
    fn end_session(&mut self) {
        for (_, pending) in self.pending.drain() {
            match pending.kind {
                PendingKind::Request(r) => drop(r.send(Err(Error::ConnectionLost))),
                PendingKind::StateOnce { responder, .. } => drop(responder.send(Err(Error::ConnectionLost))),
                PendingKind::Subscribe { sub_id, responder: Some(r) } => {
                    self.subs.remove(&sub_id);
                    let _ = r.send(Err(Error::ConnectionLost));
                }
                PendingKind::Subscribe { responder: None, .. } | PendingKind::Ignore => {}
            }
        }
        self.routes.clear();
        self.orphans.clear();
        for entry in self.subs.values_mut() {
            entry.ha_id = None;
        }
        self.ping = None;
    }

    /// Sends `request` with the next message id. The pending entry is registered
    /// first so that `end_session` can fail it if the send itself fails.
    async fn send(&mut self, ws: &mut WsStream, request: Request, kind: PendingKind) -> Result<u64, String> {
        let id = self.next_msg_id;
        self.next_msg_id += 1;
        let deadline = Instant::now() + self.shared.config.request_timeout;
        self.pending.insert(id, Pending { kind, deadline });
        self.send_raw(ws, id, request).await?;
        Ok(id)
    }

    async fn send_raw(&mut self, ws: &mut WsStream, id: u64, request: Request) -> Result<(), String> {
        let text = protocol::with_id(id, request);
        log::trace!(">>> {text}");
        ws.send(Message::Text(text.into()))
            .await
            .map_err(|e| format!("failed to send: {e}"))
    }

    /// Re-sends every subscription, in creation order, with fresh ids.
    async fn resubscribe(&mut self, ws: &mut WsStream) -> Result<(), String> {
        let subs: Vec<(u64, Request)> = self.subs.iter().map(|(id, s)| (*id, s.request.clone())).collect();
        if !subs.is_empty() {
            log::info!("Restoring {} subscription(s)...", subs.len());
        }
        for (sub_id, request) in subs {
            self.send(ws, request, PendingKind::Subscribe { sub_id, responder: None }).await?;
        }
        Ok(())
    }

    async fn handle_command(&mut self, ws: &mut WsStream, cmd: Command) -> Result<(), String> {
        match cmd {
            Command::Request { request, responder } => {
                self.send(ws, request, PendingKind::Request(responder)).await?;
            }
            Command::GetStateOnce { entity_id, responder } => {
                let request = protocol::subscribe_entities(&[&entity_id]);
                self.send(ws, request, PendingKind::StateOnce { entity_id, responder }).await?;
            }
            Command::Subscribe { kind, request, callback, responder } => {
                let sub_id = self.next_sub_id;
                self.next_sub_id += 1;
                self.subs.insert(
                    sub_id,
                    SubEntry { kind, request: request.clone(), events: spawn_worker(sub_id, callback), ha_id: None },
                );
                self.send(ws, request, PendingKind::Subscribe { sub_id, responder: Some(responder) }).await?;
            }
            Command::Unsubscribe { id, responder } => match self.subs.remove(&id).and_then(|s| s.ha_id) {
                Some(ha_id) => {
                    self.routes.remove(&ha_id);
                    self.send(ws, protocol::unsubscribe(ha_id), PendingKind::Request(responder)).await?;
                }
                // Not yet acknowledged: `handle_result` unsubscribes once the ack arrives.
                None => drop(responder.send(Ok(Value::Null))),
            },
        }
        Ok(())
    }

    async fn handle_text(&mut self, ws: &mut WsStream, text: &str) -> Result<(), String> {
        log::trace!("<<< {text}");
        let msg: Incoming = match serde_json::from_str(text) {
            Ok(msg) => msg,
            Err(e) => {
                log::warn!("Ignoring unparsable message from Home Assistant: {e}");
                return Ok(());
            }
        };
        let Some(id) = msg.id else {
            log::debug!("Ignoring message without id: {}", msg.msg_type);
            return Ok(());
        };
        match msg.msg_type.as_str() {
            "result" => {
                let outcome = if msg.success == Some(true) {
                    Ok(msg.result.unwrap_or(Value::Null))
                } else {
                    Err(msg.error.map(Error::from).unwrap_or_else(|| {
                        Error::Protocol("request failed without an error message".into())
                    }))
                };
                self.handle_result(ws, id, outcome).await
            }
            "event" => self.handle_event(ws, id, msg.event.unwrap_or(Value::Null)).await,
            "pong" => {
                if self.ping.is_some_and(|(ping_id, _)| ping_id == id) {
                    self.ping = None;
                }
                Ok(())
            }
            other => {
                log::debug!("Ignoring message of type '{other}'");
                Ok(())
            }
        }
    }

    async fn handle_result(&mut self, ws: &mut WsStream, id: u64, outcome: Result<Value>) -> Result<(), String> {
        let Some(pending) = self.pending.remove(&id) else {
            log::debug!("Result for unknown or expired request id {id}");
            return Ok(());
        };
        match pending.kind {
            PendingKind::Request(r) => drop(r.send(outcome)),
            PendingKind::Subscribe { sub_id, responder } => match outcome {
                Ok(_) => match self.subs.get_mut(&sub_id) {
                    Some(entry) => {
                        entry.ha_id = Some(id);
                        self.routes.insert(id, sub_id);
                        if let Some(r) = responder {
                            let _ = r.send(Ok(Subscription { id: sub_id }));
                        }
                    }
                    // Unsubscribed while the ack was in flight.
                    None => {
                        self.send(ws, protocol::unsubscribe(id), PendingKind::Ignore).await?;
                    }
                },
                Err(e) => {
                    self.subs.remove(&sub_id);
                    match responder {
                        Some(r) => drop(r.send(Err(e))),
                        None => log::error!("Home Assistant rejected re-subscription {sub_id}: {e}. Dropping it."),
                    }
                }
            },
            PendingKind::StateOnce { entity_id, responder } => match outcome {
                // Acknowledged; the state arrives as the first event.
                Ok(_) => {
                    let kind = PendingKind::StateOnce { entity_id, responder };
                    self.pending.insert(id, Pending { kind, deadline: pending.deadline });
                }
                Err(e) => drop(responder.send(Err(e))),
            },
            PendingKind::Ignore => {
                if let Err(e) = outcome {
                    log::debug!("Internal request {id} failed: {e}");
                }
            }
        }
        Ok(())
    }

    async fn handle_event(&mut self, ws: &mut WsStream, id: u64, payload: Value) -> Result<(), String> {
        if matches!(self.pending.get(&id), Some(Pending { kind: PendingKind::StateOnce { .. }, .. })) {
            let Some(Pending { kind: PendingKind::StateOnce { entity_id, responder }, .. }) = self.pending.remove(&id)
            else {
                unreachable!()
            };
            let state = match payload.get("a").and_then(|a| a.get(&entity_id)) {
                Some(compressed) => State::from_compressed(&entity_id, compressed).map_err(Error::from),
                None => Err(Error::Ha { code: "not_found".into(), message: format!("Entity '{entity_id}' not found") }),
            };
            let _ = responder.send(state);
            return self.send(ws, protocol::unsubscribe(id), PendingKind::Ignore).await.map(drop);
        }

        let Some(entry) = self.routes.get(&id).and_then(|sub_id| self.subs.get(sub_id)) else {
            // E.g. a subscription whose request timed out but which Home Assistant
            // created anyway. Cancel it once so it stops sending events.
            if self.orphans.insert(id) {
                log::debug!("Event for unknown subscription id {id}; unsubscribing");
                self.send(ws, protocol::unsubscribe(id), PendingKind::Ignore).await?;
            }
            return Ok(());
        };
        let event = entry.kind.convert(payload);
        if let Some(client) = self.client() {
            let _ = entry.events.send((client, event));
        }
        Ok(())
    }

    /// Expires timed-out requests and checks the connection with `ping`.
    async fn housekeeping(&mut self, ws: &mut WsStream) -> Result<(), String> {
        let now = Instant::now();
        let expired: Vec<u64> = self.pending.iter().filter(|(_, p)| p.deadline <= now).map(|(id, _)| *id).collect();
        for id in expired {
            let Some(pending) = self.pending.remove(&id) else { continue };
            match pending.kind {
                PendingKind::Request(r) => drop(r.send(Err(Error::Timeout))),
                PendingKind::StateOnce { responder, .. } => drop(responder.send(Err(Error::Timeout))),
                PendingKind::Subscribe { sub_id, responder } => {
                    self.subs.remove(&sub_id);
                    match responder {
                        Some(r) => drop(r.send(Err(Error::Timeout))),
                        None => log::error!("Re-subscription {sub_id} timed out. Dropping it."),
                    }
                }
                PendingKind::Ignore => {}
            }
        }

        let config = &self.shared.config;
        match self.ping {
            Some((_, sent)) if now.duration_since(sent) >= config.pong_timeout => {
                return Err(format!("no pong within {:?}", config.pong_timeout));
            }
            None if now.duration_since(self.last_rx) >= config.ping_interval => {
                let id = self.next_msg_id;
                self.next_msg_id += 1;
                self.ping = Some((id, now));
                self.send_raw(ws, id, protocol::ping()).await?;
            }
            _ => {}
        }
        Ok(())
    }
}

/// Resolves once shutdown is requested. Returns `()` so no (non-`Send`) watch guard
/// is held inside `select!`.
async fn stopped(shutdown_rx: &mut watch::Receiver<bool>) {
    let _ = shutdown_rx.wait_for(|stop| *stop).await;
}

/// Runs a subscription's callbacks sequentially, so events are handled in order.
/// Each callback runs in its own task, so a panic is contained and logged.
fn spawn_worker(sub_id: u64, callback: EventCallback) -> mpsc::UnboundedSender<(HAClient, Event)> {
    let (tx, mut rx) = mpsc::unbounded_channel::<(HAClient, Event)>();
    tokio::spawn(async move {
        while let Some((client, event)) = rx.recv().await {
            match tokio::spawn(callback(client, event)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => log::error!("Callback of subscription {sub_id} failed: {e:#}"),
                Err(e) => log::error!("Callback of subscription {sub_id} panicked: {e}"),
            }
        }
    });
    tx
}

/// Housekeeping granularity: fine enough for the configured timeouts, at most 1s.
fn tick_period(config: &ClientConfig) -> Duration {
    [config.request_timeout, config.ping_interval, config.pong_timeout]
        .into_iter()
        .map(|d| d / 4)
        .fold(Duration::from_secs(1), Duration::min)
        .max(Duration::from_millis(10))
}

/// Exponential backoff with ±20% jitter.
fn backoff(config: &ClientConfig, attempt: u32) -> Duration {
    let base = config
        .reconnect_min_delay
        .saturating_mul(2u32.saturating_pow(attempt.saturating_sub(1).min(16)))
        .min(config.reconnect_max_delay);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let jitter = 0.8 + 0.4 * (nanos % 1000) as f64 / 1000.0;
    base.mul_f64(jitter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        let cfg = ClientConfig::new("ws://x", "t").reconnect_delay(Duration::from_secs(1), Duration::from_secs(60));
        let within = |d: Duration, base: f64| d.as_secs_f64() >= base * 0.8 && d.as_secs_f64() <= base * 1.2;
        assert!(within(backoff(&cfg, 1), 1.0));
        assert!(within(backoff(&cfg, 3), 4.0));
        assert!(within(backoff(&cfg, 30), 60.0));
    }
}
