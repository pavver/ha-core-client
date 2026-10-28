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
use crate::events::{Event, State, SubscriptionStatus};
use crate::protocol::{self, Incoming, Request};
use crate::queue::{self, QueueRx, QueueTx};
use crate::subscription::{Command, EntityChanges, EventCallback, SubKind, Subscription};
use crate::{ConnectionState, HAClient};

/// State shared between the runner and every [`HAClient`] handle.
pub(crate) struct Shared {
    pub(crate) config: ClientConfig,
    pub(crate) shutdown_tx: watch::Sender<bool>,
    pub(crate) state_tx: watch::Sender<ConnectionState>,
    /// Number of subscription workers still running, so `stop()` can wait for them.
    pub(crate) workers: watch::Sender<usize>,
}

tokio::task_local! {
    /// Set while a subscription callback runs, so that `stop()` called from one
    /// does not wait for the callback itself to finish.
    static IN_CALLBACK: ();
}

/// Whether the current task is a subscription callback.
pub(crate) fn in_callback() -> bool {
    IN_CALLBACK.try_with(|_| ()).is_ok()
}

struct SubEntry {
    kind: SubKind,
    request: Request,
    /// Feeds the subscription's worker task, which runs callbacks one at a time.
    events: QueueTx,
    /// Message id of the subscription on the current connection, once acknowledged.
    ha_id: Option<u64>,
    /// `Interrupted` was delivered and `Restored` not yet.
    interrupted: bool,
    /// Last state seen by an entity subscription, to detect changes missed while disconnected.
    last_state: Option<State>,
    /// Events of an entity subscription that arrive while its state is re-read after a
    /// reconnect. Delivered after `Restored`, without those the re-read already covers.
    held: Option<Vec<Event>>,
    /// Events that arrived before Home Assistant acknowledged the subscription (it sends a
    /// template's first render this way). Delivered once the subscription is confirmed.
    early: Vec<Event>,
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
    /// Reads the state of an entity subscription: the baseline after subscribing,
    /// or the current state after a reconnect (`restoring`). On the initial subscribe,
    /// `responder` is answered only after this read, so a missing entity is an error.
    Resync {
        sub_id: u64,
        restoring: bool,
        responder: Option<oneshot::Sender<Result<Subscription>>>,
    },
    /// `unsubscribe_events` of `ha_id`; `responder` is `None` for internal cleanups.
    Unsubscribe {
        ha_id: u64,
        responder: Option<oneshot::Sender<Result<Value>>>,
    },
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
    /// Server-side subscriptions we unsubscribed from; late events for them are ignored.
    orphans: HashSet<u64>,
    last_rx: Instant,
    ping: Option<(u64, Instant)>,
}

impl Drop for Runner {
    fn drop(&mut self) {
        // Reached without `finish` only if the task panicked: make sure `closed()`/`stop()`
        // waiters are released and new requests fail with `Stopped`.
        if !self.shared.state_tx.borrow().is_terminal() {
            log::error!("Home Assistant client task ended unexpectedly.");
            self.shared.state_tx.send_replace(ConnectionState::Stopped);
        }
    }
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
            let started = Instant::now();
            let end = self.session(&mut ws, &mut cmd_rx, &mut shutdown_rx).await;
            // The backoff starts over only after a session that stayed up, so a connection
            // that Home Assistant drops right after `auth_ok` keeps backing off.
            if started.elapsed() >= stable_session(&self.shared.config) {
                attempt = 0;
            }
            // A deliberate stop reports `Closed` (in `finish`), not `Interrupted`.
            self.end_session(matches!(end, SessionEnd::Lost(_)));
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
        let ids: Vec<u64> = self.subs.keys().copied().collect();
        for sub_id in ids {
            // Dropping the entry ends its worker task once queued events are handled.
            self.close_subscription(sub_id, SubscriptionStatus::Closed);
        }
        self.set_state(state);
        log::info!("Home Assistant client stopped.");
    }

    fn set_state(&self, state: ConnectionState) {
        self.shared.state_tx.send_replace(state);
    }

    /// Delivers `Restored`, followed by the events held during the state re-read.
    fn restored(&mut self, sub_id: u64) {
        let Some(entry) = self.subs.get_mut(&sub_id) else { return };
        entry.interrupted = false;
        entry.events.push(status_event(sub_id, SubscriptionStatus::Restored));
        for event in entry.held.take().unwrap_or_default() {
            entry.push_entity_event(event, true);
        }
    }

    /// Registers a subscription and starts its worker. Not yet sent to Home Assistant.
    fn add_subscription(&mut self, kind: SubKind, request: Request, callback: EventCallback) -> u64 {
        let sub_id = self.next_sub_id;
        self.next_sub_id += 1;
        let filter = match kind {
            SubKind::EntityState { changes: EntityChanges::State, .. } => queue::Filter::StateChanges,
            SubKind::EntityState { .. } => queue::Filter::Merge,
            _ => queue::Filter::None,
        };
        let (events, rx) = queue::channel(sub_id, self.shared.config.subscription_queue_size, filter);
        tokio::spawn(Worker::new(rx, callback, self.weak_tx.clone(), self.shared.clone()).run());
        let entry =
            SubEntry { kind, request, events, ha_id: None, interrupted: false, last_state: None, held: None, early: Vec::new() };
        self.subs.insert(sub_id, entry);
        sub_id
    }

    /// Hands the subscription to its caller. If the caller stopped waiting (timed out or
    /// dropped the future), nobody could ever unsubscribe it, so it is removed; the
    /// returned Home Assistant id, if any, must then be unsubscribed.
    fn confirm(&mut self, sub_id: u64, responder: oneshot::Sender<Result<Subscription>>) -> Option<u64> {
        if responder.send(Ok(Subscription { id: sub_id })).is_ok() {
            return None;
        }
        log::debug!("Subscription {sub_id} was abandoned by its caller; removing it");
        self.remove_subscription(sub_id)
    }

    /// Removes a subscription that was never handed out: queued events are discarded and
    /// the callback is not told. Returns its Home Assistant id.
    fn remove_subscription(&mut self, sub_id: u64) -> Option<u64> {
        let entry = self.subs.remove(&sub_id)?;
        entry.events.discard();
        let ha_id = entry.ha_id?;
        self.routes.remove(&ha_id);
        Some(ha_id)
    }

    /// Removes a subscription after queueing `status` as its last event.
    /// Returns its Home Assistant id.
    fn close_subscription(&mut self, sub_id: u64, status: SubscriptionStatus) -> Option<u64> {
        let entry = self.subs.remove(&sub_id)?;
        entry.events.push(status_event(sub_id, status));
        let ha_id = entry.ha_id?;
        self.routes.remove(&ha_id);
        Some(ha_id)
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
            // Kept and sent with the others once reconnected; starts out `Interrupted`.
            Command::Subscribe { kind, request, callback, responder } => {
                let sub_id = self.add_subscription(kind, request, callback);
                self.confirm(sub_id, responder);
                if let Some(entry) = self.subs.get_mut(&sub_id) {
                    entry.interrupted = true;
                    entry.events.push(status_event(sub_id, SubscriptionStatus::Interrupted));
                }
            }
            Command::Unsubscribe { id, responder } => {
                self.close_subscription(id, SubscriptionStatus::Unsubscribed);
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

        if let Some(end) = self.resubscribe(ws, shutdown_rx).await {
            return end;
        }
        self.set_state(ConnectionState::Connected);

        let mut tick = tokio::time::interval(tick_period(&self.shared.config));
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            // Not `biased`: a steady stream of frames must not starve commands and housekeeping.
            let step = tokio::select! {
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

    /// Fails everything that was in flight on the connection that just ended and,
    /// if `interrupt`, tells every subscription that it is interrupted.
    /// `interrupt` is false for a deliberate stop: requests then fail with `Stopped`
    /// and subscriptions get `Closed` (in `finish`) instead of `Interrupted`.
    fn end_session(&mut self, interrupt: bool) {
        let error = || if interrupt { Error::ConnectionLost } else { Error::Stopped };
        let pending: Vec<Pending> = self.pending.drain().map(|(_, p)| p).collect();
        for pending in pending {
            match pending.kind {
                PendingKind::Request(r) => drop(r.send(Err(error()))),
                PendingKind::StateOnce { responder, .. } => drop(responder.send(Err(error()))),
                PendingKind::Subscribe { sub_id, responder: Some(r) }
                | PendingKind::Resync { sub_id, responder: Some(r), .. } => {
                    if interrupt {
                        // As if it had been made while disconnected: restored on the next connection.
                        self.confirm(sub_id, r);
                    } else {
                        self.remove_subscription(sub_id);
                        let _ = r.send(Err(Error::Stopped));
                    }
                }
                // The server-side subscription ended with the connection.
                PendingKind::Unsubscribe { responder: Some(r), .. } => drop(r.send(Ok(Value::Null))),
                PendingKind::Subscribe { responder: None, .. }
                | PendingKind::Resync { .. }
                | PendingKind::Unsubscribe { .. } => {}
            }
        }
        self.routes.clear();
        self.orphans.clear();
        self.ping = None;

        for (sub_id, entry) in self.subs.iter_mut() {
            entry.ha_id = None;
            // The next re-read reports everything that changed since `last_state`.
            entry.held = None;
            entry.early.clear();
            if interrupt && !entry.interrupted {
                entry.interrupted = true;
                entry.events.push(status_event(*sub_id, SubscriptionStatus::Interrupted));
            }
        }
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

    /// Writes one message. A write that does not complete within `pong_timeout`
    /// (e.g. a half-open connection with a full send buffer) ends the session,
    /// instead of blocking the runner until the OS gives up on the socket.
    async fn send_raw(&mut self, ws: &mut WsStream, id: u64, request: Request) -> Result<(), String> {
        let text = protocol::with_id(id, request);
        log::trace!(">>> {text}");
        let timeout = self.shared.config.pong_timeout;
        match tokio::time::timeout(timeout, ws.send(Message::Text(text.into()))).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(format!("failed to send: {e}")),
            Err(_) => Err(format!("write did not complete within {timeout:?}")),
        }
    }

    /// Unsubscribes from a server-side subscription we no longer route.
    async fn unsubscribe_orphan(&mut self, ws: &mut WsStream, ha_id: u64) -> Result<(), String> {
        self.unsubscribe(ws, ha_id, None).await
    }

    async fn unsubscribe(
        &mut self,
        ws: &mut WsStream,
        ha_id: u64,
        responder: Option<oneshot::Sender<Result<Value>>>,
    ) -> Result<(), String> {
        // Events already in flight for it are ignored until Home Assistant confirms.
        self.orphans.insert(ha_id);
        self.send(ws, protocol::unsubscribe(ha_id), PendingKind::Unsubscribe { ha_id, responder }).await.map(drop)
    }

    /// Re-sends every subscription, in creation order, with fresh ids.
    /// Returns how the session ended if it could not finish.
    async fn resubscribe(&mut self, ws: &mut WsStream, shutdown_rx: &watch::Receiver<bool>) -> Option<SessionEnd> {
        let subs: Vec<(u64, Request)> = self.subs.iter().map(|(id, s)| (*id, s.request.clone())).collect();
        if !subs.is_empty() {
            log::info!("Restoring {} subscription(s)...", subs.len());
        }
        for (sub_id, request) in subs {
            if *shutdown_rx.borrow() {
                return Some(SessionEnd::Stop);
            }
            if let Err(reason) = self.send(ws, request, PendingKind::Subscribe { sub_id, responder: None }).await {
                return Some(SessionEnd::Lost(reason));
            }
        }
        None
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
            // The caller gave up while the command was queued.
            Command::Subscribe { responder, .. } if responder.is_closed() => {}
            Command::Subscribe { kind, request, callback, responder } => {
                let sub_id = self.add_subscription(kind, request.clone(), callback);
                self.send(ws, request, PendingKind::Subscribe { sub_id, responder: Some(responder) }).await?;
            }
            Command::Unsubscribe { id, responder } => {
                match self.close_subscription(id, SubscriptionStatus::Unsubscribed) {
                    Some(ha_id) => self.unsubscribe(ws, ha_id, Some(responder)).await?,
                    // Not yet acknowledged: `handle_result` unsubscribes once the ack arrives.
                    None => drop(responder.send(Ok(Value::Null))),
                }
            }
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
            PendingKind::Unsubscribe { ha_id, responder } => {
                // Home Assistant sends nothing more for it after this answer.
                self.orphans.remove(&ha_id);
                match (responder, outcome) {
                    (Some(r), outcome) => drop(r.send(outcome)),
                    (None, Err(e)) => log::debug!("Unsubscribing {ha_id} failed: {e}"),
                    (None, Ok(_)) => {}
                }
            }
            PendingKind::Subscribe { sub_id, responder } => match outcome {
                Ok(_) => self.subscription_acknowledged(ws, id, sub_id, responder).await?,
                Err(e) => match responder {
                    Some(r) => {
                        self.remove_subscription(sub_id);
                        let _ = r.send(Err(e));
                    }
                    None => {
                        log::error!("Home Assistant rejected re-subscription {sub_id}: {e}. Dropping it.");
                        let (code, message) = match e {
                            Error::Ha { code, message } => (code, message),
                            other => ("error".to_string(), other.to_string()),
                        };
                        self.close_subscription(sub_id, SubscriptionStatus::Rejected { code, message });
                    }
                },
            },
            // Acknowledged; the state arrives as the first event.
            kind @ (PendingKind::StateOnce { .. } | PendingKind::Resync { .. }) if outcome.is_ok() => {
                self.pending.insert(id, Pending { kind, deadline: pending.deadline });
            }
            PendingKind::StateOnce { responder, .. } => {
                if let Err(e) = outcome {
                    let _ = responder.send(Err(e));
                }
            }
            PendingKind::Resync { sub_id, restoring, responder } => {
                if let Err(e) = outcome {
                    log::warn!("Could not read the state of subscription {sub_id}: {e}");
                }
                if let Some(ha_id) = self.resync_failed(sub_id, restoring, responder) {
                    self.unsubscribe_orphan(ws, ha_id).await?;
                }
            }
        }
        Ok(())
    }

    /// Completes a state read that produced no usable state. The subscription itself
    /// is active; only the baseline is unknown. Returns a Home Assistant id to unsubscribe.
    fn resync_failed(
        &mut self,
        sub_id: u64,
        restoring: bool,
        responder: Option<oneshot::Sender<Result<Subscription>>>,
    ) -> Option<u64> {
        let abandoned = responder.and_then(|r| self.confirm(sub_id, r));
        if restoring {
            self.restored(sub_id);
        }
        abandoned
    }

    async fn subscription_acknowledged(
        &mut self,
        ws: &mut WsStream,
        ha_id: u64,
        sub_id: u64,
        responder: Option<oneshot::Sender<Result<Subscription>>>,
    ) -> Result<(), String> {
        let restoring = responder.is_none();
        let Some(entry) = self.subs.get_mut(&sub_id) else {
            // Unsubscribed while the ack was in flight.
            return self.unsubscribe_orphan(ws, ha_id).await;
        };
        entry.ha_id = Some(ha_id);
        let read = match &entry.kind {
            SubKind::EntityState { entity_id, .. } => Some(protocol::subscribe_entities(&[entity_id])),
            _ => None,
        };
        let early = std::mem::take(&mut entry.early);
        if read.is_some() && restoring {
            // Live events wait until the re-read tells what was missed meanwhile.
            entry.held = Some(early);
        } else if read.is_some() {
            // The baseline read that follows already includes them.
        } else {
            entry.early = early;
        }
        self.routes.insert(ha_id, sub_id);
        match read {
            // Read the entity's state: the baseline now, or what changed while disconnected.
            Some(request) => {
                self.send(ws, request, PendingKind::Resync { sub_id, restoring, responder }).await?;
            }
            None => {
                if let Some(ha_id) = responder.and_then(|r| self.confirm(sub_id, r)) {
                    self.unsubscribe_orphan(ws, ha_id).await?;
                }
                if restoring {
                    self.restored(sub_id);
                }
                if let Some(entry) = self.subs.get_mut(&sub_id) {
                    for event in std::mem::take(&mut entry.early) {
                        entry.events.push(event);
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_event(&mut self, ws: &mut WsStream, id: u64, payload: Value) -> Result<(), String> {
        match self.pending.get(&id).map(|p| &p.kind) {
            Some(PendingKind::StateOnce { .. }) => {
                let Some(Pending { kind: PendingKind::StateOnce { entity_id, responder }, .. }) =
                    self.pending.remove(&id)
                else {
                    unreachable!()
                };
                let state = match read_compressed(&payload, &entity_id) {
                    Ok(Some(state)) => Ok(state),
                    Ok(None) => Err(Error::Ha { code: "not_found".into(), message: format!("Entity '{entity_id}' not found") }),
                    Err(e) => Err(Error::from(e)),
                };
                let _ = responder.send(state);
                return self.unsubscribe_orphan(ws, id).await;
            }
            Some(PendingKind::Resync { .. }) => {
                let Some(Pending { kind: PendingKind::Resync { sub_id, restoring, responder }, .. }) =
                    self.pending.remove(&id)
                else {
                    unreachable!()
                };
                if let Some(dropped) = self.apply_resync(sub_id, restoring, responder, &payload) {
                    self.unsubscribe_orphan(ws, dropped).await?;
                }
                return self.unsubscribe_orphan(ws, id).await;
            }
            Some(&PendingKind::Subscribe { sub_id, .. }) => {
                if let Some(entry) = self.subs.get_mut(&sub_id) {
                    let event = entry.kind.convert(payload);
                    entry.early.push(event);
                }
                return Ok(());
            }
            _ => {}
        }

        let Some(sub_id) = self.routes.get(&id).copied() else {
            // E.g. a subscription whose request timed out but which Home Assistant
            // created anyway. Cancel it once so it stops sending events.
            if !self.orphans.contains(&id) {
                log::debug!("Event for unknown subscription id {id}; unsubscribing");
                self.unsubscribe_orphan(ws, id).await?;
            }
            return Ok(());
        };
        let Some(entry) = self.subs.get_mut(&sub_id) else { return Ok(()) };
        let event = entry.kind.convert(payload);
        if let Some(held) = &mut entry.held {
            held.push(event);
        } else {
            entry.push_entity_event(event, false);
        }
        Ok(())
    }

    /// Handles the state read for an entity subscription: records the baseline, and after
    /// a reconnect reports a change that happened while disconnected, then `Restored`.
    ///
    /// On the initial subscribe (`responder` set), a missing entity fails the subscription.
    /// A returned Home Assistant subscription id (of a failed or abandoned subscription)
    /// must be unsubscribed.
    fn apply_resync(
        &mut self,
        sub_id: u64,
        restoring: bool,
        responder: Option<oneshot::Sender<Result<Subscription>>>,
        payload: &Value,
    ) -> Option<u64> {
        let entry = self.subs.get_mut(&sub_id)?;
        let SubKind::EntityState { entity_id, .. } = &entry.kind else { return None };
        let entity_id = entity_id.clone();
        let current = match read_compressed(payload, &entity_id) {
            Ok(state) => state,
            Err(e) => {
                log::warn!("Could not parse the state of {entity_id}: {e}");
                return self.resync_failed(sub_id, restoring, responder);
            }
        };
        if let Some(r) = responder {
            if current.is_none() {
                let ha_id = self.remove_subscription(sub_id);
                let message = format!("Entity '{entity_id}' not found");
                let _ = r.send(Err(Error::Ha { code: "not_found".into(), message }));
                return ha_id;
            }
            entry.last_state = current;
            return self.confirm(sub_id, r);
        }
        let missed = restoring && changed(entry.last_state.as_ref(), current.as_ref());
        let old_state = std::mem::replace(&mut entry.last_state, current.clone());
        if missed {
            entry.events.push(Event::StateChanged { entity_id, old_state, new_state: current });
        }
        if restoring {
            self.restored(sub_id);
        }
        None
    }

    /// Expires timed-out requests and checks the connection with `ping`.
    async fn housekeeping(&mut self, ws: &mut WsStream) -> Result<(), String> {
        let now = Instant::now();
        let expired: Vec<u64> = self.pending.iter().filter(|(_, p)| p.deadline <= now).map(|(id, _)| *id).collect();
        let mut resubscribe_timed_out = false;
        let mut abandoned = Vec::new();
        for id in expired {
            let Some(pending) = self.pending.remove(&id) else { continue };
            match pending.kind {
                PendingKind::Request(r) => drop(r.send(Err(Error::Timeout))),
                PendingKind::StateOnce { responder, .. } => drop(responder.send(Err(Error::Timeout))),
                PendingKind::Subscribe { sub_id, responder: Some(r) } => {
                    self.remove_subscription(sub_id);
                    let _ = r.send(Err(Error::Timeout));
                }
                // Keep the subscription: a reconnect will retry it.
                PendingKind::Subscribe { responder: None, .. } => resubscribe_timed_out = true,
                PendingKind::Resync { sub_id, responder: Some(r), .. } => {
                    abandoned.extend(self.remove_subscription(sub_id));
                    let _ = r.send(Err(Error::Timeout));
                }
                PendingKind::Resync { sub_id, restoring, responder: None } => {
                    if restoring {
                        self.restored(sub_id);
                    }
                }
                PendingKind::Unsubscribe { responder, .. } => {
                    if let Some(r) = responder {
                        let _ = r.send(Err(Error::Timeout));
                    }
                }
            }
        }
        for ha_id in abandoned {
            self.unsubscribe_orphan(ws, ha_id).await?;
        }
        if resubscribe_timed_out {
            return Err("Home Assistant did not confirm restored subscriptions in time".into());
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

/// Reads `entity_id` from a `subscribe_entities` initial event (`{"a": {entity: compressed}}`).
fn read_compressed(payload: &Value, entity_id: &str) -> serde_json::Result<Option<State>> {
    payload
        .get("a")
        .and_then(|a| a.get(entity_id))
        .map(|compressed| State::from_compressed(entity_id, compressed))
        .transpose()
}

impl SubEntry {
    /// Queues an event, tracking `last_state` for entity subscriptions. A state change
    /// that `last_state` already includes is skipped: the same update (seen by both the
    /// state re-read and the trigger) or, with `or_older`, an earlier one.
    fn push_entity_event(&mut self, event: Event, or_older: bool) {
        if let (SubKind::EntityState { .. }, Event::StateChanged { new_state, .. }) = (&self.kind, &event) {
            if covered(self.last_state.as_ref(), new_state.as_ref(), or_older) {
                return;
            }
            self.last_state = new_state.clone();
        }
        self.events.push(event);
    }
}

fn status_event(sub_id: u64, status: SubscriptionStatus) -> Event {
    Event::Status { subscription: Subscription { id: sub_id }, status }
}

/// Timestamps are compared with 1ms tolerance because `subscribe_entities` sends them as floats.
const TIMESTAMP_TOLERANCE: chrono::TimeDelta = chrono::TimeDelta::milliseconds(1);

/// Whether `last` already reflects the update to `new`: the same update or, with
/// `or_older`, any update not newer than `last`.
fn covered(last: Option<&State>, new: Option<&State>, or_older: bool) -> bool {
    let (Some(last), Some(new)) = (last, new) else { return false };
    let newer_by = new.last_updated - last.last_updated;
    if or_older {
        newer_by <= TIMESTAMP_TOLERANCE
    } else {
        last.state == new.state && newer_by.abs() <= TIMESTAMP_TOLERANCE
    }
}

/// Whether the entity differs from the last known state. Without a baseline nothing is reported.
fn changed(last: Option<&State>, current: Option<&State>) -> bool {
    last.is_some() && !covered(last, current, false)
}

/// Resolves once shutdown is requested. Returns `()` so no (non-`Send`) watch guard
/// is held inside `select!`.
async fn stopped(shutdown_rx: &mut watch::Receiver<bool>) {
    let _ = shutdown_rx.wait_for(|stop| *stop).await;
}

/// Runs a subscription's callbacks sequentially, so events are handled in order.
struct Worker {
    rx: QueueRx,
    callback: EventCallback,
    /// Weak so that queued events don't keep the client alive.
    weak_tx: mpsc::WeakSender<Command>,
    shared: Arc<Shared>,
}

impl Worker {
    /// Counted in `Shared::workers` until dropped.
    fn new(rx: QueueRx, callback: EventCallback, weak_tx: mpsc::WeakSender<Command>, shared: Arc<Shared>) -> Self {
        shared.workers.send_modify(|n| *n += 1);
        Self { rx, callback, weak_tx, shared }
    }

    async fn run(self) {
        let sub_id = self.rx.sub_id();
        while let Some(event) = self.rx.recv().await {
            // All handles dropped: the client is shutting down; drain silently.
            let Some(cmd_tx) = self.weak_tx.upgrade() else { continue };
            let client = HAClient { cmd_tx, shared: self.shared.clone() };
            // Each callback runs in its own task, so a panic is contained and logged.
            let task = tokio::spawn(IN_CALLBACK.scope((), (self.callback)(client, event)));
            match self.watch_slow(sub_id, task).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => log::error!("Callback of subscription {sub_id} failed: {}", error_chain(&*e)),
                Err(e) => log::error!("Callback of subscription {sub_id} panicked: {e}"),
            }
        }
    }

    /// Awaits `task`, warning once if it runs longer than `slow_callback_warning`.
    async fn watch_slow<T>(
        &self,
        sub_id: u64,
        mut task: tokio::task::JoinHandle<T>,
    ) -> Result<T, tokio::task::JoinError> {
        let threshold = self.shared.config.slow_callback_warning;
        if threshold.is_zero() {
            return task.await;
        }
        let started = Instant::now();
        tokio::select! {
            result = &mut task => return result,
            _ = tokio::time::sleep(threshold) => log::warn!(
                "Callback of subscription {sub_id} has been running for {threshold:?}; {} event(s) waiting",
                self.rx.len()
            ),
        }
        let result = task.await;
        log::warn!("Slow callback of subscription {sub_id} finished after {:?}", started.elapsed());
        result
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.shared.workers.send_modify(|n| *n -= 1);
    }
}

/// The error followed by its sources, like anyhow's `{:#}`.
fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut out = error.to_string();
    let mut source = error.source();
    while let Some(e) = source {
        out.push_str(": ");
        out.push_str(&e.to_string());
        source = e.source();
    }
    out
}

/// Housekeeping granularity: fine enough for the configured timeouts, at most 1s.
fn tick_period(config: &ClientConfig) -> Duration {
    [config.request_timeout, config.ping_interval, config.pong_timeout]
        .into_iter()
        .map(|d| d / 4)
        .fold(Duration::from_secs(1), Duration::min)
        .max(Duration::from_millis(10))
}

/// How long a session must last for the reconnect backoff to start over: the longest
/// backoff delay, at most a minute.
fn stable_session(config: &ClientConfig) -> Duration {
    config.reconnect_max_delay.min(Duration::from_secs(60))
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
    use serde_json::json;

    #[test]
    fn backoff_grows_and_caps() {
        let cfg = ClientConfig::new("ws://x", "t").reconnect_delay(Duration::from_secs(1), Duration::from_secs(60));
        let within = |d: Duration, base: f64| d.as_secs_f64() >= base * 0.8 && d.as_secs_f64() <= base * 1.2;
        assert!(within(backoff(&cfg, 1), 1.0));
        assert!(within(backoff(&cfg, 3), 4.0));
        assert!(within(backoff(&cfg, 30), 60.0));
    }

    #[test]
    fn huge_durations_do_not_overflow() {
        let cfg = ClientConfig::new("ws://x", "t")
            .request_timeout(Duration::MAX)
            .ping_interval(Duration::MAX)
            .pong_timeout(Duration::MAX)
            .reconnect_delay(Duration::MAX, Duration::MAX);
        let _ = Instant::now() + cfg.request_timeout;
        let _ = backoff(&cfg, u32::MAX);
        assert_eq!(tick_period(&cfg), Duration::from_secs(1));
    }

    #[test]
    fn change_detection() {
        let s = |state: &str, lu: f64| State::from_compressed("sensor.x", &json!({"s": state, "lc": lu, "lu": lu})).unwrap();
        assert!(!changed(None, Some(&s("1", 1.0))));
        assert!(changed(Some(&s("1", 1.0)), None));
        assert!(changed(Some(&s("1", 1.0)), Some(&s("2", 1.0))));
        assert!(changed(Some(&s("1", 1.0)), Some(&s("1", 2.0))));
        assert!(!changed(Some(&s("1", 1.0000004)), Some(&s("1", 1.0))));
    }

    #[test]
    fn coverage_of_held_and_duplicate_changes() {
        let s = |state: &str, lu: f64| State::from_compressed("sensor.x", &json!({"s": state, "lc": lu, "lu": lu})).unwrap();
        // Same update seen twice.
        assert!(covered(Some(&s("2", 5.0)), Some(&s("2", 5.0)), false));
        // A newer update with the same state (e.g. attributes changed) is not a duplicate.
        assert!(!covered(Some(&s("2", 5.0)), Some(&s("2", 6.0)), false));
        // Held events older than the re-read are covered by it; newer ones are not.
        assert!(covered(Some(&s("3", 5.0)), Some(&s("2", 4.0)), true));
        assert!(!covered(Some(&s("3", 5.0)), Some(&s("4", 6.0)), true));
        assert!(!covered(None, Some(&s("1", 1.0)), true));
    }
}
