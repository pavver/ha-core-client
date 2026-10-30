//! The sans-IO core: [`Session`] holds all protocol state and performs no I/O.
//!
//! A `Session` owns the Home Assistant WebSocket protocol: authentication, message ids,
//! subscriptions with their re-subscription and state resync after a reconnect, request
//! bookkeeping, keep-alive pings and the reconnect backoff. It never touches a socket and
//! never reads the clock: the embedder (the host) opens the WebSocket, feeds it what
//! happens and writes out what it asks for. `HAClient`
//! is exactly such an embedder, built on Tokio; the same core runs where there is no
//! runtime, for instance inside a WebAssembly component that is called once per tick.
//!
//! # The embedder's loop
//!
//! ```
//! use std::time::Instant;
//! use ha_core_client::{Action, ClientConfig, Session, Source, Update};
//!
//! let mut session = Session::new(ClientConfig::for_session("TOKEN"));
//! let now = Instant::now();
//! let mut pending = session.subscribe(now, Source::entity("light.kitchen"));
//! let mut events = None;
//!
//! // One iteration per tick (or per I/O event). The host owns the socket.
//! while let Some(action) = session.poll_action(now) {
//!     match action {
//!         Action::Connect => { /* open the WebSocket, then: session.on_connected(now) */ }
//!         Action::Close => { /* drop the socket */ }
//!         _ => {}
//!     }
//! }
//! // For each received text frame:       session.on_frame(now, &text);
//! // When the socket fails or closes:    session.on_disconnected(now, "reason");
//! session.tick(now);
//! while let Some(frame) = session.poll_transmit() {
//!     // Write `frame` to the socket; on a write error call `on_disconnected`.
//!     # let _ = frame;
//! }
//! if events.is_none() {
//!     if let Some(Ok(established)) = pending.poll() {
//!         events = Some(established);
//!     }
//! }
//! if let Some(events) = &mut events {
//!     for update in events.drain() {
//!         match update {
//!             Update::Event(change) => println!("{} changed", change.entity_id),
//!             Update::Status { status, .. } => println!("subscription status: {status:?}"),
//!             _ => {}
//!         }
//!     }
//! }
//! ```
//!
//! Times are passed in (`now: Instant`, any monotonic clock the host has), so the core is
//! deterministic and testable. Every call is cheap and never blocks.
//!
//! Results that `HAClient` returns from `async` methods come back as [`Reply`] (one-shot
//! requests) or [`PendingSubscription`] / [`SessionEvents`] (subscriptions), which the
//! embedder polls on a later tick. Subscription statuses (`Interrupted`, `Restored`,
//! `Rejected`, `Lagged`, ...) arrive in order with the data events, as
//! [`Update::Status`], exactly as they do for callbacks and streams.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::Value;
use serde_json::value::RawValue;
use tokio::sync::oneshot;

use crate::config::ClientConfig;
use crate::error::{Error, HaErrorCode, Result};
use crate::events::{State, StateChange, SubEvent, SubscriptionStatus, Update};
use crate::protocol::{self, Incoming, Request};
use crate::queue::{self, QueueRx, QueueTx, TryRecvError};
use crate::source::{into_update, Source};
use crate::state::{Domain, Service};
use crate::subscription::{Command, Delivery, EntityChanges, SubKind, Subscribed, Subscription};
use crate::target::Target;

/// Connection status of an `HAClient`, see `HAClient::watch_connection_state`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionState {
    /// The first connection has not been established yet (`HAClient::connect_lazy`).
    /// Requests fail with [`Error::NotConnected`]; subscriptions are accepted and start
    /// `Interrupted`. A failed first attempt continues as `Reconnecting`.
    Connecting,
    /// Connected and authenticated. After a reconnect, subscriptions are re-sent at this
    /// point; each one is active again once it reports [`SubscriptionStatus::Restored`].
    Connected,
    /// The connection was lost; requests fail with [`Error::NotConnected`] until it is back.
    /// `attempt` counts reconnects since the last session that stayed up (for the longest
    /// backoff delay, at most a minute), so a connection that keeps dropping right after
    /// connecting backs off instead of retrying at the shortest delay.
    Reconnecting { attempt: u32 },
    /// Home Assistant rejected the token (while connecting or reconnecting). Terminal.
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


/// What the embedder must do with the transport, from [`Session::poll_action`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Action {
    /// Open a WebSocket to Home Assistant, then call [`Session::on_connected`] (or
    /// [`Session::on_disconnected`] if opening fails). Returned when the first connection
    /// is due, and again after each backoff delay.
    Connect,
    /// Drop the current WebSocket (or abandon the attempt to open one). The session has
    /// already treated the connection as ended and schedules the reconnect itself, so
    /// there is no need to call [`Session::on_disconnected`] afterwards (it is harmless).
    /// Always handled before the next [`Action::Connect`].
    Close,
}

enum HandshakeStage {
    /// Waiting for Home Assistant's `auth_required`.
    AwaitAuthRequired,
    /// `auth` sent; waiting for `auth_ok` or `auth_invalid`.
    AwaitAuthOk,
}

enum Phase {
    /// No transport. `connect_at` is when the next connect is due; `None`: now.
    Disconnected { connect_at: Option<Instant> },
    /// `Action::Connect` was handed out; waiting for `on_connected`.
    Connecting { deadline: Instant },
    Handshake { stage: HandshakeStage, deadline: Instant },
    Connected,
    /// Stopped or authentication failed. Never left.
    Terminal,
}

/// Answers a `Subscribe` command once the subscription is established.
type SubscribeResponder = oneshot::Sender<Result<Subscribed>>;

struct SubEntry {
    kind: SubKind,
    request: Request,
    /// Feeds the subscription's consumer (a worker task or a [`SessionEvents`]).
    events: QueueTx,
    /// The receiving end of a stream subscription, until `confirm` hands it to the caller.
    stream_rx: Option<QueueRx>,
    /// Message id of the subscription on the current connection, once acknowledged.
    ha_id: Option<u64>,
    /// `Interrupted` was delivered and `Restored` not yet.
    interrupted: bool,
    /// Last state seen by an entity subscription, to detect changes missed while disconnected.
    /// `None` until known; `Some(None)` while the entity does not exist (it was removed).
    last_state: Option<Option<State>>,
    /// Events of an entity subscription that arrive while its state is re-read after a
    /// reconnect. Delivered after `Restored`, without those the re-read already covers.
    held: Option<Vec<SubEvent>>,
    /// Events that arrived before Home Assistant acknowledged the subscription (it sends a
    /// template's first render this way). Delivered once the subscription is confirmed.
    early: Vec<SubEvent>,
}

fn null_result() -> Box<RawValue> {
    RawValue::from_string("null".into()).expect("null is valid JSON")
}

enum PendingKind {
    Request(oneshot::Sender<Result<Box<RawValue>>>),
    /// `responder` is `None` for re-subscriptions after a reconnect.
    Subscribe {
        sub_id: u64,
        responder: Option<SubscribeResponder>,
    },
    /// `subscribe_entities` used to read one state; answered by the first event.
    StateOnce {
        entity_id: String,
        responder: oneshot::Sender<Result<State>>,
    },
    /// Reads the baseline state of an entity subscription made while connected.
    /// `responder` is answered only after this read, so a missing entity is an error.
    Resync {
        sub_id: u64,
        responder: Option<SubscribeResponder>,
    },
    /// One `subscribe_entities` for all entity subscriptions restored after a reconnect;
    /// reports what each missed, then `Restored`.
    ResyncBatch {
        sub_ids: Vec<u64>,
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

/// The result of a one-shot request, available later. Poll it on a following tick.
///
/// Dropping a `Reply` before the request was handed to the session cancels it; once sent,
/// the answer is simply discarded.
#[must_use = "poll the reply to learn the result"]
pub struct Reply<T> {
    poll: Box<dyn FnMut() -> Option<Result<T>> + Send>,
    done: bool,
}

impl<T> Reply<T> {
    fn new<R: Send + 'static>(
        mut rx: oneshot::Receiver<Result<R>>,
        map: impl Fn(R) -> Result<T> + Send + 'static,
    ) -> Self {
        let poll = move || match rx.try_recv() {
            Ok(result) => Some(result.and_then(&map)),
            Err(oneshot::error::TryRecvError::Empty) => None,
            // The session ended without answering.
            Err(oneshot::error::TryRecvError::Closed) => Some(Err(Error::Stopped)),
        };
        Self { poll: Box::new(poll), done: false }
    }

    /// The result, once there is one (`None`: not yet). Each reply yields its result once;
    /// polling again returns `None`.
    ///
    /// Failures are the same as for the `async` client: [`Error::NotConnected`] if the
    /// session is not connected, [`Error::Timeout`] after the request timeout,
    /// [`Error::ConnectionLost`] if the connection drops first, [`Error::Ha`] for an error
    /// reply, [`Error::Stopped`] after [`Session::stop`].
    pub fn poll(&mut self) -> Option<Result<T>> {
        if self.done {
            return None;
        }
        let result = (self.poll)();
        self.done = result.is_some();
        result
    }
}

impl<T> std::fmt::Debug for Reply<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reply").field("done", &self.done).finish()
    }
}

/// A subscription that Home Assistant has not acknowledged yet, from [`Session::subscribe`].
///
/// Keep it until [`poll`](Self::poll) yields; dropping it earlier abandons the
/// subscription (it is removed and, if already sent, cancelled).
#[must_use = "poll to learn whether the subscription was established"]
pub struct PendingSubscription<T> {
    rx: oneshot::Receiver<Result<Subscribed>>,
    convert: fn(SubEvent) -> Option<T>,
    done: bool,
}

impl<T> PendingSubscription<T> {
    /// The established subscription, or why it failed (`None`: not acknowledged yet).
    /// While the session is offline the subscription is accepted at once, starting
    /// `Interrupted`, and is sent after the next connection.
    pub fn poll(&mut self) -> Option<Result<SessionEvents<T>>> {
        if self.done {
            return None;
        }
        let result = match self.rx.try_recv() {
            Ok(Ok(Subscribed { subscription, rx: Some(rx) })) => Ok(SessionEvents { subscription, rx, convert: self.convert }),
            Ok(Ok(Subscribed { rx: None, .. })) => Err(Error::Stopped),
            Ok(Err(e)) => Err(e),
            Err(oneshot::error::TryRecvError::Empty) => return None,
            Err(oneshot::error::TryRecvError::Closed) => Err(Error::Stopped),
        };
        self.done = true;
        Some(result)
    }
}

/// The updates of an established subscription, drained synchronously.
///
/// Yields data events and status changes ([`Update::Status`]: `Interrupted`, `Restored`,
/// `Rejected`, `Lagged`, `Unsubscribed`, `Closed`) in order, like a callback would receive
/// them. Merging and the queue limit work as for callbacks
/// ([`ClientConfig::subscription_queue_size`]). It does not unsubscribe when dropped: call
/// [`Session::unsubscribe`].
pub struct SessionEvents<T> {
    subscription: Subscription,
    rx: QueueRx,
    convert: fn(SubEvent) -> Option<T>,
}

impl<T> SessionEvents<T> {
    /// The subscription, for [`Session::unsubscribe`].
    pub fn subscription(&self) -> &Subscription {
        &self.subscription
    }

    /// The next update, or `None` if nothing is waiting right now.
    pub fn try_next(&mut self) -> Option<Update<T>> {
        loop {
            match self.rx.try_recv() {
                Ok(event) => {
                    if let Some(update) = into_update(self.convert, event, self.subscription.id) {
                        return Some(update);
                    }
                }
                Err(TryRecvError::Empty | TryRecvError::Closed) => return None,
            }
        }
    }

    /// An iterator over the updates waiting now.
    pub fn drain(&mut self) -> impl Iterator<Item = Update<T>> + '_ {
        std::iter::from_fn(move || self.try_next())
    }

    /// `true` once the subscription ended (`Closed`, `Unsubscribed` or `Rejected` was
    /// delivered) and everything was drained: nothing will ever arrive.
    pub fn is_finished(&self) -> bool {
        self.rx.is_finished()
    }
}

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

/// The sans-IO Home Assistant client core. See the [module documentation](crate::session).
pub struct Session {
    config: ClientConfig,
    /// Unique per session, so that a [`Subscription`] of another session is recognised.
    client_id: u64,
    phase: Phase,
    state: ConnectionState,
    /// The time passed to the call being handled; internal steps use it instead of
    /// threading a parameter through every handler.
    now: Option<Instant>,
    /// Reconnect attempts since the last session that stayed up.
    attempt: u32,
    connected_at: Option<Instant>,
    /// Why the last connection attempt failed, for the caller that awaits it.
    connect_error: Option<Error>,
    /// Frames to write to the transport, in order.
    outbox: VecDeque<String>,
    actions: VecDeque<Action>,

    subs: BTreeMap<u64, SubEntry>,
    next_sub_id: u64,

    // Per-connection state; reset by `end_session`.
    next_msg_id: u64,
    pending: HashMap<u64, Pending>,
    routes: HashMap<u64, u64>,
    /// Server-side subscriptions we unsubscribed from; late events for them are ignored.
    orphans: HashSet<u64>,
    /// Requests that create a server-side subscription and timed out unanswered. Should
    /// Home Assistant still accept one, it is unsubscribed at once, not only at its first
    /// event (a rarely firing trigger would otherwise stay until the connection ends).
    expired_subscribes: HashSet<u64>,
    /// Entity subscriptions whose re-subscription is not yet answered.
    restoring: BTreeSet<u64>,
    /// Entity subscriptions re-subscribed and waiting (their events held) for the batched
    /// state read, which is sent once `restoring` is empty.
    restore_acked: Vec<u64>,
    last_rx: Option<Instant>,
    ping: Option<(u64, Instant)>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("state", &self.state)
            .field("subscriptions", &self.subs.len())
            .field("pending", &self.pending.len())
            .finish()
    }
}

impl Session {
    /// Creates a session that wants to connect right away: the first [`poll_action`]
    /// returns [`Action::Connect`]. `config.url` is not used; only the token and the
    /// timeouts are (see [`ClientConfig::for_session`]).
    ///
    /// [`poll_action`]: Self::poll_action
    pub fn new(config: ClientConfig) -> Self {
        Self {
            config,
            client_id: NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed),
            phase: Phase::Disconnected { connect_at: None },
            state: ConnectionState::Connecting,
            now: None,
            attempt: 0,
            connected_at: None,
            connect_error: None,
            outbox: VecDeque::new(),
            actions: VecDeque::new(),
            subs: BTreeMap::new(),
            next_sub_id: 1,
            next_msg_id: 1,
            pending: HashMap::new(),
            routes: HashMap::new(),
            orphans: HashSet::new(),
            expired_subscribes: HashSet::new(),
            restoring: BTreeSet::new(),
            restore_acked: Vec::new(),
            last_rx: None,
            ping: None,
        }
    }

    pub(crate) fn client_id(&self) -> u64 {
        self.client_id
    }

    pub(crate) fn config(&self) -> &ClientConfig {
        &self.config
    }

    fn now(&self) -> Instant {
        self.now.expect("every entry point records the current time first")
    }

    // --- Transport interface ---

    /// The current connection state.
    pub fn connection_state(&self) -> ConnectionState {
        self.state.clone()
    }

    /// Whether a transport is in use: authenticating or connected.
    pub(crate) fn has_transport(&self) -> bool {
        matches!(self.phase, Phase::Handshake { .. } | Phase::Connected)
    }

    /// Whether authentication is done and requests can be sent.
    pub fn is_connected(&self) -> bool {
        matches!(self.phase, Phase::Connected)
    }

    /// What the embedder must do with the transport next, if anything. Call it in a loop
    /// until it returns `None`, on every tick.
    pub fn poll_action(&mut self, now: Instant) -> Option<Action> {
        self.now = Some(now);
        if let Some(action) = self.actions.pop_front() {
            return Some(action);
        }
        if let Phase::Disconnected { connect_at } = self.phase {
            if connect_at.is_none_or(|at| now >= at) {
                self.phase = Phase::Connecting { deadline: now + self.config.connect_timeout };
                return Some(Action::Connect);
            }
        }
        None
    }

    /// The next time the session needs a call to [`tick`](Self::tick) or
    /// [`poll_action`](Self::poll_action) even if nothing happens, or `None` if it is idle.
    /// A host that is called regularly (every 100 ms, say) may ignore it.
    pub fn next_wakeup(&self) -> Option<Instant> {
        match &self.phase {
            Phase::Disconnected { connect_at } => *connect_at,
            Phase::Connecting { deadline } | Phase::Handshake { deadline, .. } => Some(*deadline),
            Phase::Terminal => None,
            Phase::Connected => {
                let deadlines = self.pending.values().map(|p| p.deadline);
                let ping = match self.ping {
                    Some((_, sent)) => Some(sent + self.config.pong_timeout),
                    None => self.last_rx.map(|rx| rx + self.config.ping_interval),
                };
                deadlines.chain(ping).min()
            }
        }
    }

    /// The transport is open (the WebSocket handshake is done). Authentication follows
    /// from the frames Home Assistant sends.
    pub fn on_connected(&mut self, now: Instant) {
        self.now = Some(now);
        match self.phase {
            Phase::Disconnected { .. } | Phase::Connecting { .. } => {
                self.outbox.clear();
                self.phase = Phase::Handshake {
                    stage: HandshakeStage::AwaitAuthRequired,
                    deadline: now + self.config.connect_timeout,
                };
            }
            _ => log::debug!("Ignoring on_connected: no connection is being established"),
        }
    }

    /// The transport failed or closed: a failed connect, a `Close` frame, a read or write
    /// error. Ignored if the session has already ended that connection. Everything in
    /// flight fails with [`Error::ConnectionLost`], subscriptions become `Interrupted`
    /// and the reconnect is scheduled with backoff.
    pub fn on_disconnected(&mut self, now: Instant, reason: &str) {
        self.now = Some(now);
        self.connection_ended(reason, false);
    }

    /// Any traffic that is not a text frame (ping, pong, binary) counts as a sign of life.
    pub fn on_activity(&mut self, now: Instant) {
        self.now = Some(now);
        if self.is_connected() {
            self.last_rx = Some(now);
        }
    }

    /// A text frame arrived from Home Assistant.
    pub fn on_frame(&mut self, now: Instant, text: &str) {
        self.now = Some(now);
        match &self.phase {
            Phase::Handshake { .. } => self.handshake_frame(text),
            Phase::Connected => {
                self.last_rx = Some(now);
                self.handle_text(text);
            }
            _ => log::debug!("Ignoring a frame received while not connected"),
        }
    }

    /// Time passes: expires overdue requests, sends keep-alive pings, gives up on a
    /// connect that takes too long. Call it regularly; every 100 ms to 1 s is plenty (a
    /// quarter of the shortest configured timeout, at most 1 s, is what `HAClient` uses).
    pub fn tick(&mut self, now: Instant) {
        self.now = Some(now);
        match self.phase {
            Phase::Connecting { deadline } | Phase::Handshake { deadline, .. } if now >= deadline => {
                self.connection_ended("timed out connecting", true);
            }
            Phase::Connected => self.housekeeping(),
            _ => {}
        }
    }

    /// The next frame to write to the WebSocket, in order. Call it until it returns `None`.
    /// If a write fails, call [`on_disconnected`](Self::on_disconnected): the rest is
    /// discarded with the connection.
    pub fn poll_transmit(&mut self) -> Option<String> {
        self.outbox.pop_front()
    }

    /// Stops the session for good: in-flight requests fail with [`Error::Stopped`] and
    /// subscriptions end with [`SubscriptionStatus::Closed`]. The state becomes
    /// [`ConnectionState::Stopped`]; a pending [`Action::Close`] tells the host to drop the socket.
    pub fn stop(&mut self, now: Instant) {
        self.now = Some(now);
        if matches!(self.phase, Phase::Terminal) {
            return;
        }
        if !matches!(self.phase, Phase::Disconnected { .. }) {
            self.actions.push_back(Action::Close);
        }
        self.end_session(false);
        self.finish(ConnectionState::Stopped);
    }

    /// The error of the last failed connection attempt, if the caller wants it (the first
    /// connect of `HAClient` reports it instead of retrying).
    #[cfg_attr(not(feature = "runtime"), allow(dead_code))]
    pub(crate) fn take_connect_error(&mut self) -> Option<Error> {
        self.connect_error.take()
    }

    fn set_state(&mut self, state: ConnectionState) {
        self.state = state;
    }

    /// Ends the session for good: every subscription gets `Closed`.
    fn finish(&mut self, state: ConnectionState) {
        let ids: Vec<u64> = self.subs.keys().copied().collect();
        for sub_id in ids {
            // Dropping the entry ends its consumer once queued events are handled.
            self.close_subscription(sub_id, SubscriptionStatus::Closed);
        }
        self.outbox.clear();
        self.phase = Phase::Terminal;
        self.set_state(state);
        log::info!("Home Assistant client stopped.");
    }

    // --- Connection lifecycle ---

    /// The connection (or the attempt to make one) is over. `close`: the host must drop its
    /// socket (the session ended it, the host did not notice).
    fn connection_ended(&mut self, reason: &str, close: bool) {
        let now = self.now();
        match self.phase {
            Phase::Connected => {
                // The backoff starts over only after a session that stayed up, so a connection
                // that Home Assistant drops right after `auth_ok` keeps backing off.
                if self.connected_at.is_some_and(|at| now.duration_since(at) >= stable_session(&self.config)) {
                    self.attempt = 0;
                }
                self.end_session(true);
                log::warn!("Connection to Home Assistant lost: {reason}");
            }
            Phase::Connecting { .. } | Phase::Handshake { .. } => {
                log::warn!("Connection attempt to Home Assistant failed: {reason}");
            }
            Phase::Disconnected { .. } | Phase::Terminal => return,
        }
        self.outbox.clear();
        if close {
            self.actions.push_back(Action::Close);
        }
        self.attempt += 1;
        self.set_state(ConnectionState::Reconnecting { attempt: self.attempt });
        let delay = backoff(&self.config, self.attempt);
        log::info!("Reconnecting in {delay:?} (attempt {})...", self.attempt);
        self.phase = Phase::Disconnected { connect_at: Some(now + delay) };
    }

    /// A failed handshake: remembered for the caller, then handled like any lost connection.
    fn handshake_failed(&mut self, error: Error) {
        let reason = error.to_string();
        self.connect_error = Some(error);
        self.connection_ended(&reason, true);
    }

    fn handshake_frame(&mut self, text: &str) {
        let msg: Incoming = match serde_json::from_str(text) {
            Ok(msg) => msg,
            Err(e) => return self.handshake_failed(Error::Protocol(format!("invalid handshake message: {e}"))),
        };
        let Phase::Handshake { stage, .. } = &self.phase else { return };
        match stage {
            HandshakeStage::AwaitAuthRequired => {
                if msg.msg_type != "auth_required" {
                    let error = Error::Protocol(format!("expected 'auth_required', got '{}'", msg.msg_type));
                    return self.handshake_failed(error);
                }
                // The token is never logged.
                self.outbox.push_back(protocol::auth(&self.config.token));
                if let Phase::Handshake { stage, .. } = &mut self.phase {
                    *stage = HandshakeStage::AwaitAuthOk;
                }
            }
            HandshakeStage::AwaitAuthOk => match msg.msg_type.as_str() {
                "auth_ok" => {
                    log::info!("Connected and authenticated to Home Assistant");
                    if let Some(version) = msg.ha_version.as_deref().filter(|v| !is_supported(v)) {
                        log::warn!(
                            "Home Assistant {version} is older than {}.{}; trigger subscriptions will be rejected.",
                            MIN_HA_VERSION.0, MIN_HA_VERSION.1
                        );
                    }
                    self.start_session();
                }
                "auth_invalid" => {
                    let message = msg.message.unwrap_or_else(|| "invalid access token".into());
                    // Retrying a rejected token only risks an IP ban.
                    log::error!("Home Assistant rejected the access token: {message}. Giving up.");
                    self.connect_error = Some(Error::Auth(message.clone()));
                    self.actions.push_back(Action::Close);
                    self.finish(ConnectionState::AuthFailed(message));
                }
                other => {
                    let error = Error::Protocol(format!("expected 'auth_ok' or 'auth_invalid', got '{other}'"));
                    self.handshake_failed(error);
                }
            },
        }
    }

    /// Authentication succeeded: ids restart, subscriptions are re-sent.
    fn start_session(&mut self) {
        let now = self.now();
        self.phase = Phase::Connected;
        self.connected_at = Some(now);
        self.connect_error = None;
        self.next_msg_id = 1;
        self.last_rx = Some(now);
        self.ping = None;
        self.resubscribe();
        self.set_state(ConnectionState::Connected);
    }

    /// Delivers `Restored`, followed by the events held during the state re-read.
    fn restored(&mut self, sub_id: u64) {
        let Some(entry) = self.subs.get_mut(&sub_id) else { return };
        entry.interrupted = false;
        entry.events.push_status(SubscriptionStatus::Restored);
        for event in entry.held.take().unwrap_or_default() {
            entry.push_entity_event(event, true);
        }
    }

    /// Registers a subscription and starts its consumer. Not yet sent to Home Assistant.
    fn add_subscription(&mut self, kind: SubKind, request: Request, delivery: Delivery) -> u64 {
        let sub_id = self.next_sub_id;
        self.next_sub_id += 1;
        let filter = match kind {
            SubKind::EntityState { changes, merge, .. } => {
                queue::Filter { merge, state_only: changes == EntityChanges::State }
            }
            _ => queue::Filter::NONE,
        };
        let subscription = Subscription { id: sub_id, client: self.client_id };
        let (events, rx) = queue::channel(subscription, self.config.subscription_queue_size, filter);
        let stream_rx = match delivery {
            Delivery::Worker(start) => {
                start(rx);
                None
            }
            Delivery::Stream => Some(rx),
        };
        let entry = SubEntry {
            kind,
            request,
            events,
            stream_rx,
            ha_id: None,
            interrupted: false,
            last_state: None,
            held: None,
            early: Vec::new(),
        };
        self.subs.insert(sub_id, entry);
        sub_id
    }

    /// Hands the subscription to its caller. If the caller stopped waiting (timed out or
    /// dropped the future), nobody could ever unsubscribe it, so it is removed; the
    /// returned Home Assistant id, if any, must then be unsubscribed.
    fn confirm(&mut self, sub_id: u64, responder: SubscribeResponder) -> Option<u64> {
        let rx = self.subs.get_mut(&sub_id).and_then(|entry| entry.stream_rx.take());
        let subscription = Subscription { id: sub_id, client: self.client_id };
        if responder.send(Ok(Subscribed { subscription, rx })).is_ok() {
            return None;
        }
        log::debug!("Subscription {sub_id} was abandoned by its caller; removing it");
        self.remove_subscription(sub_id)
    }

    /// A subscription that left no longer takes part in the batched state read.
    fn forget_restore(&mut self, sub_id: u64) {
        self.restoring.remove(&sub_id);
        self.restore_acked.retain(|id| *id != sub_id);
    }

    /// Removes a subscription that was never handed out: queued events are discarded and
    /// the callback is not told. Returns its Home Assistant id.
    fn remove_subscription(&mut self, sub_id: u64) -> Option<u64> {
        let entry = self.subs.remove(&sub_id)?;
        self.forget_restore(sub_id);
        entry.events.discard();
        let ha_id = entry.ha_id?;
        self.routes.remove(&ha_id);
        Some(ha_id)
    }

    /// Removes a subscription after queueing `status` as its last event.
    /// Returns its Home Assistant id.
    fn close_subscription(&mut self, sub_id: u64, status: SubscriptionStatus) -> Option<u64> {
        let entry = self.subs.remove(&sub_id)?;
        self.forget_restore(sub_id);
        entry.events.push_status(status);
        let ha_id = entry.ha_id?;
        self.routes.remove(&ha_id);
        Some(ha_id)
    }

    // --- Commands ---

    /// Handles a command from a user handle: sends it if connected, answers it from the
    /// offline rules otherwise.
    pub(crate) fn handle_command(&mut self, now: Instant, cmd: Command) {
        self.now = Some(now);
        match self.phase {
            Phase::Connected => self.handle_online_command(cmd),
            Phase::Terminal => Self::reject_command(cmd),
            _ => self.handle_offline_command(cmd),
        }
    }

    fn reject_command(cmd: Command) {
        match cmd {
            Command::Request { responder, .. } => drop(responder.send(Err(Error::Stopped))),
            Command::GetStateOnce { responder, .. } => drop(responder.send(Err(Error::Stopped))),
            Command::Subscribe { responder, .. } => drop(responder.send(Err(Error::Stopped))),
            Command::Unsubscribe { responder, .. } => drop(responder.send(Ok(Value::Null))),
        }
    }

    fn handle_offline_command(&mut self, cmd: Command) {
        match cmd {
            Command::Request { responder, .. } => drop(responder.send(Err(Error::NotConnected))),
            Command::GetStateOnce { responder, .. } => drop(responder.send(Err(Error::NotConnected))),
            // Kept and sent with the others once reconnected; starts out `Interrupted`.
            Command::Subscribe { kind, request, delivery, responder } => {
                let sub_id = self.add_subscription(kind, request, delivery);
                self.confirm(sub_id, responder);
                if let Some(entry) = self.subs.get_mut(&sub_id) {
                    entry.interrupted = true;
                    entry.events.push_status(SubscriptionStatus::Interrupted);
                }
            }
            Command::Unsubscribe { id, responder } => {
                self.close_subscription(id, SubscriptionStatus::Unsubscribed);
                let _ = responder.send(Ok(Value::Null));
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
                | PendingKind::ResyncBatch { .. }
                | PendingKind::Unsubscribe { .. } => {}
            }
        }
        self.routes.clear();
        self.orphans.clear();
        self.expired_subscribes.clear();
        self.restoring.clear();
        self.restore_acked.clear();
        self.ping = None;

        for entry in self.subs.values_mut() {
            entry.ha_id = None;
            // The next re-read reports everything that changed since `last_state`.
            entry.held = None;
            entry.early.clear();
            if interrupt && !entry.interrupted {
                entry.interrupted = true;
                entry.events.push_status(SubscriptionStatus::Interrupted);
            }
        }
    }

    /// Queues `request` with the next message id. The pending entry is registered
    /// first so that `end_session` can fail it if the connection is lost before the answer.
    fn send(&mut self, request: Request, kind: PendingKind) {
        let deadline = self.now() + self.config.request_timeout;
        self.send_until(request, kind, deadline);
    }

    /// Like [`send`](Self::send), with the deadline of an earlier step of the same operation.
    fn send_until(&mut self, request: Request, kind: PendingKind, deadline: Instant) {
        let id = self.next_msg_id;
        self.next_msg_id += 1;
        self.pending.insert(id, Pending { kind, deadline });
        self.send_raw(id, request);
    }

    /// Queues one message for the transport. Writing it is the host's job (and the host
    /// bounds the write: a write that never completes is a dead connection).
    fn send_raw(&mut self, id: u64, request: Request) {
        let text = protocol::with_id(id, request);
        log::trace!(">>> {text}");
        self.outbox.push_back(text);
    }

    /// Unsubscribes from a server-side subscription we no longer route.
    fn unsubscribe_orphan(&mut self, ha_id: u64) {
        self.unsubscribe_server(ha_id, None);
    }

    fn unsubscribe_server(&mut self, ha_id: u64, responder: Option<oneshot::Sender<Result<Value>>>) {
        // Events already in flight for it are ignored until Home Assistant confirms.
        self.orphans.insert(ha_id);
        self.send(protocol::unsubscribe(ha_id), PendingKind::Unsubscribe { ha_id, responder });
    }

    /// Re-sends every subscription, in creation order, with fresh ids.
    fn resubscribe(&mut self) {
        let subs: Vec<(u64, Request)> = self.subs.iter().map(|(id, s)| (*id, s.request.clone())).collect();
        if !subs.is_empty() {
            log::info!("Restoring {} subscription(s)...", subs.len());
        }
        self.restoring = self
            .subs
            .iter()
            .filter(|(_, s)| matches!(s.kind, SubKind::EntityState { .. }))
            .map(|(id, _)| *id)
            .collect();
        self.restore_acked.clear();
        for (sub_id, request) in subs {
            self.send(request, PendingKind::Subscribe { sub_id, responder: None });
        }
    }

    fn handle_online_command(&mut self, cmd: Command) {
        match cmd {
            // The caller gave up while the command was queued: it has reported a timeout, so the
            // request must not take effect afterwards (a service call would run unseen).
            Command::Request { responder, .. } if responder.is_closed() => {}
            Command::GetStateOnce { responder, .. } if responder.is_closed() => {}
            Command::Request { request, responder } => {
                self.send(request, PendingKind::Request(responder));
            }
            Command::GetStateOnce { entity_id, responder } => {
                let request = protocol::subscribe_entities(&[&entity_id]);
                self.send(request, PendingKind::StateOnce { entity_id, responder });
            }
            // The caller gave up while the command was queued.
            Command::Subscribe { responder, .. } if responder.is_closed() => {}
            Command::Subscribe { kind, request, delivery, responder } => {
                let sub_id = self.add_subscription(kind, request.clone(), delivery);
                self.send(request, PendingKind::Subscribe { sub_id, responder: Some(responder) });
            }
            Command::Unsubscribe { id, responder } => {
                match self.close_subscription(id, SubscriptionStatus::Unsubscribed) {
                    Some(ha_id) => self.unsubscribe_server(ha_id, Some(responder)),
                    // Not yet acknowledged: `handle_result` unsubscribes once the ack arrives.
                    None => drop(responder.send(Ok(Value::Null))),
                }
            }
        }
        self.send_restore_read();
    }

    /// Once every entity subscription has been re-subscribed (or has left), reads the
    /// current state of all of them with one `subscribe_entities`.
    fn send_restore_read(&mut self) {
        if !self.restoring.is_empty() || self.restore_acked.is_empty() {
            return;
        }
        let sub_ids = std::mem::take(&mut self.restore_acked);
        // Several subscriptions may watch the same entity.
        let entity_ids: BTreeSet<&str> = sub_ids
            .iter()
            .filter_map(|id| match &self.subs.get(id)?.kind {
                SubKind::EntityState { entity_id, .. } => Some(entity_id.as_str()),
                _ => None,
            })
            .collect();
        let request = protocol::subscribe_entities(&entity_ids.into_iter().collect::<Vec<_>>());
        self.send(request, PendingKind::ResyncBatch { sub_ids });
    }

    fn handle_text(&mut self, text: &str) {
        log::trace!("<<< {text}");
        let msg: Incoming = match serde_json::from_str(text) {
            Ok(msg) => msg,
            Err(e) => {
                log::warn!("Ignoring unparsable message from Home Assistant: {e}");
                return;
            }
        };
        let Some(id) = msg.id else {
            log::debug!("Ignoring message without id: {}", msg.msg_type);
            return;
        };
        match msg.msg_type.as_str() {
            "result" => {
                let outcome = if msg.success == Some(true) {
                    Ok(msg.result.unwrap_or_else(null_result))
                } else {
                    Err(msg.error.map(Error::from).unwrap_or_else(|| {
                        Error::Protocol("request failed without an error message".into())
                    }))
                };
                self.handle_result(id, outcome)
            }
            "event" => self.handle_event(id, msg.event.unwrap_or(Value::Null)),
            "pong" => {
                if self.ping.is_some_and(|(ping_id, _)| ping_id == id) {
                    self.ping = None;
                }
            }
            other => log::debug!("Ignoring message of type '{other}'"),
        }
    }

    fn handle_result(&mut self, id: u64, outcome: Result<Box<RawValue>>) {
        self.process_result(id, outcome);
        // The result may have been the last outstanding re-subscription.
        self.send_restore_read();
    }

    fn process_result(&mut self, id: u64, outcome: Result<Box<RawValue>>) {
        let Some(pending) = self.pending.remove(&id) else {
            if self.expired_subscribes.remove(&id) && outcome.is_ok() && !self.orphans.contains(&id) {
                log::debug!("Late acknowledgement of expired subscription request {id}; unsubscribing");
                return self.unsubscribe_orphan(id);
            }
            log::debug!("Result for unknown or expired request id {id}");
            return;
        };
        match pending.kind {
            PendingKind::Request(r) => drop(r.send(outcome)),
            PendingKind::Unsubscribe { ha_id, responder } => {
                // Home Assistant sends nothing more for it after this answer.
                self.orphans.remove(&ha_id);
                match (responder, outcome) {
                    (Some(r), outcome) => drop(r.send(outcome.map(|_| Value::Null))),
                    (None, Err(e)) => log::debug!("Unsubscribing {ha_id} failed: {e}"),
                    (None, Ok(_)) => {}
                }
            }
            PendingKind::Subscribe { sub_id, responder } => match outcome {
                Ok(_) => self.subscription_acknowledged(id, sub_id, responder, pending.deadline),
                Err(e) => match responder {
                    Some(r) => {
                        self.remove_subscription(sub_id);
                        let _ = r.send(Err(e));
                    }
                    None => {
                        log::error!("Home Assistant rejected re-subscription {sub_id}: {e}. Dropping it.");
                        let (code, message) = match e {
                            Error::Ha { code, message, .. } => (code, message),
                            other => (HaErrorCode::UnknownError, other.to_string()),
                        };
                        self.close_subscription(sub_id, SubscriptionStatus::Rejected { code, message });
                    }
                },
            },
            // Acknowledged; the state arrives as the first event.
            kind @ (PendingKind::StateOnce { .. } | PendingKind::Resync { .. } | PendingKind::ResyncBatch { .. })
                if outcome.is_ok() =>
            {
                self.pending.insert(id, Pending { kind, deadline: pending.deadline });
            }
            PendingKind::StateOnce { responder, .. } => {
                if let Err(e) = outcome {
                    let _ = responder.send(Err(e));
                }
            }
            PendingKind::Resync { sub_id, responder } => {
                if let Err(e) = outcome {
                    log::warn!("Could not read the state of subscription {sub_id}: {e}");
                }
                if let Some(ha_id) = self.resync_failed(sub_id, false, responder) {
                    self.unsubscribe_orphan(ha_id);
                }
            }
            PendingKind::ResyncBatch { sub_ids } => {
                if let Err(e) = outcome {
                    log::warn!("Could not read the state of the restored entity subscriptions: {e}");
                }
                for sub_id in sub_ids {
                    self.restored(sub_id);
                }
            }
        }
    }

    /// Completes a state read that produced no usable state. The subscription itself
    /// is active; only the baseline is unknown. Returns a Home Assistant id to unsubscribe.
    fn resync_failed(
        &mut self,
        sub_id: u64,
        restoring: bool,
        responder: Option<SubscribeResponder>,
    ) -> Option<u64> {
        let abandoned = responder.and_then(|r| self.confirm(sub_id, r));
        if restoring {
            self.restored(sub_id);
        }
        abandoned
    }

    fn subscription_acknowledged(
        &mut self,
        ha_id: u64,
        sub_id: u64,
        responder: Option<SubscribeResponder>,
        deadline: Instant,
    ) {
        let restoring = responder.is_none();
        let Some(entry) = self.subs.get_mut(&sub_id) else {
            // Unsubscribed while the ack was in flight.
            return self.unsubscribe_orphan(ha_id);
        };
        entry.ha_id = Some(ha_id);
        let read = match &entry.kind {
            SubKind::EntityState { entity_id, .. } => Some(protocol::subscribe_entities(&[entity_id])),
            _ => None,
        };
        let early = std::mem::take(&mut entry.early);
        if read.is_some() && restoring {
            // Live events wait until the batched re-read tells what was missed meanwhile.
            entry.held = Some(early);
            self.routes.insert(ha_id, sub_id);
            self.restoring.remove(&sub_id);
            self.restore_acked.push(sub_id);
            return;
        } else if read.is_some() {
            // The baseline read that follows already includes them.
        } else {
            entry.early = early;
        }
        self.routes.insert(ha_id, sub_id);
        match read {
            // Read the entity's state: the baseline now, or what changed while disconnected.
            // Within the subscribe's deadline, so that the caller's timeout covers both steps.
            Some(request) => {
                let kind = PendingKind::Resync { sub_id, responder };
                self.send_until(request, kind, deadline);
            }
            None => {
                if let Some(ha_id) = responder.and_then(|r| self.confirm(sub_id, r)) {
                    self.unsubscribe_orphan(ha_id);
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
    }

    fn handle_event(&mut self, id: u64, payload: Value) {
        match self.pending.get(&id).map(|p| &p.kind) {
            Some(PendingKind::StateOnce { .. }) => {
                let Some(Pending { kind: PendingKind::StateOnce { entity_id, responder }, .. }) =
                    self.pending.remove(&id)
                else {
                    unreachable!()
                };
                let state = match read_compressed(&payload, &entity_id) {
                    Ok(Some(state)) => Ok(state),
                    Ok(None) => Err(Error::ha(HaErrorCode::NotFound, format!("Entity '{entity_id}' not found"))),
                    Err(e) => Err(Error::from(e)),
                };
                let _ = responder.send(state);
                return self.unsubscribe_orphan(id);
            }
            Some(PendingKind::Resync { .. }) => {
                let Some(Pending { kind: PendingKind::Resync { sub_id, responder }, .. }) = self.pending.remove(&id)
                else {
                    unreachable!()
                };
                if let Some(dropped) = self.apply_resync(sub_id, false, responder, &payload) {
                    self.unsubscribe_orphan(dropped);
                }
                return self.unsubscribe_orphan(id);
            }
            Some(PendingKind::ResyncBatch { .. }) => {
                let Some(Pending { kind: PendingKind::ResyncBatch { sub_ids }, .. }) = self.pending.remove(&id) else {
                    unreachable!()
                };
                for sub_id in sub_ids {
                    self.apply_resync(sub_id, true, None, &payload);
                }
                return self.unsubscribe_orphan(id);
            }
            Some(&PendingKind::Subscribe { sub_id, .. }) => {
                if let Some(entry) = self.subs.get_mut(&sub_id) {
                    let event = entry.kind.convert(payload);
                    entry.early.push(event);
                }
                return;
            }
            _ => {}
        }

        let Some(sub_id) = self.routes.get(&id).copied() else {
            // E.g. a subscription whose request timed out but which Home Assistant
            // created anyway. Cancel it once so it stops sending events.
            if !self.orphans.contains(&id) {
                log::debug!("Event for unknown subscription id {id}; unsubscribing");
                self.unsubscribe_orphan(id);
            }
            return;
        };
        let Some(entry) = self.subs.get_mut(&sub_id) else { return };
        let event = entry.kind.convert(payload);
        if let Some(held) = &mut entry.held {
            held.push(event);
        } else {
            entry.push_entity_event(event, false);
        }
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
        responder: Option<SubscribeResponder>,
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
                let _ = r.send(Err(Error::ha(HaErrorCode::NotFound, message)));
                return ha_id;
            }
            entry.last_state = Some(current);
            return self.confirm(sub_id, r);
        }
        let missed = restoring && changed(entry.last_state.as_ref().map(Option::as_ref), current.as_ref());
        let old_state = entry.last_state.replace(current.clone()).flatten();
        if missed {
            entry.events.push(SubEvent::StateChanged(StateChange { entity_id, old_state, new_state: current }));
        }
        if restoring {
            self.restored(sub_id);
        }
        None
    }

    /// Expires timed-out requests and checks the connection with `ping`.
    fn housekeeping(&mut self) {
        let now = self.now();
        let expired: Vec<u64> = self.pending.iter().filter(|(_, p)| p.deadline <= now).map(|(id, _)| *id).collect();
        let mut retry = Vec::new();
        let mut abandoned = Vec::new();
        for id in expired {
            let Some(pending) = self.pending.remove(&id) else { continue };
            if !matches!(pending.kind, PendingKind::Request(_) | PendingKind::Unsubscribe { .. }) {
                self.expired_subscribes.insert(id);
            }
            match pending.kind {
                PendingKind::Request(r) => drop(r.send(Err(Error::Timeout))),
                PendingKind::StateOnce { responder, .. } => drop(responder.send(Err(Error::Timeout))),
                PendingKind::Subscribe { sub_id, responder: Some(r) } => {
                    self.remove_subscription(sub_id);
                    let _ = r.send(Err(Error::Timeout));
                }
                // Retried on this connection rather than by reconnecting, which would
                // interrupt every other subscription too. Should Home Assistant still
                // accept the old request, it is unsubscribed (`expired_subscribes`).
                // The batched state read of the other restored entity subscriptions does not
                // wait for it: it gets its own once acknowledged.
                PendingKind::Subscribe { sub_id, responder: None } => {
                    self.restoring.remove(&sub_id);
                    retry.push(sub_id);
                }
                PendingKind::Resync { sub_id, responder: Some(r), .. } => {
                    abandoned.extend(self.remove_subscription(sub_id));
                    let _ = r.send(Err(Error::Timeout));
                }
                PendingKind::Resync { responder: None, .. } => {}
                PendingKind::ResyncBatch { sub_ids } => {
                    for sub_id in sub_ids {
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
            self.unsubscribe_orphan(ha_id);
        }
        // A removed subscription may have been the last one the batched read waited for.
        self.send_restore_read();
        for sub_id in retry {
            let Some(entry) = self.subs.get_mut(&sub_id) else { continue };
            log::warn!("Home Assistant did not confirm restored subscription {sub_id} in time; retrying");
            entry.early.clear();
            let request = entry.request.clone();
            self.send(request, PendingKind::Subscribe { sub_id, responder: None });
        }

        let config = &self.config;
        let last_rx = self.last_rx.unwrap_or(now);
        match self.ping {
            Some((_, sent)) if now.duration_since(sent) >= config.pong_timeout => {
                let reason = format!("no pong within {:?}", config.pong_timeout);
                self.connection_ended(&reason, true);
            }
            None if now.duration_since(last_rx) >= config.ping_interval => {
                let id = self.next_msg_id;
                self.next_msg_id += 1;
                self.ping = Some((id, now));
                self.send_raw(id, protocol::ping());
            }
            _ => {}
        }
    }

    // --- Requests and subscriptions for the embedder ---

    /// Sends any WebSocket command and returns its raw `result` later. An escape hatch for
    /// commands this crate has no method for. `request` must serialize to a JSON object
    /// with a string `type` and without `id`; it must not be `auth`, `unsubscribe_events`
    /// or create a subscription (use [`subscribe`](Self::subscribe) with
    /// [`Source::raw`]); otherwise [`Error::InvalidInput`]. Decode the result with
    /// `serde_json::from_str(raw.get())`.
    pub fn command(&mut self, now: Instant, request: impl Serialize) -> Result<Reply<Box<RawValue>>> {
        Ok(self.request(now, protocol::user_command(request)?))
    }

    fn request(&mut self, now: Instant, request: Request) -> Reply<Box<RawValue>> {
        let (responder, rx) = oneshot::channel();
        self.handle_command(now, Command::Request { request, responder });
        Reply::new(rx, Ok)
    }

    /// Fetches the state of all entities (entities whose state cannot be parsed are skipped).
    pub fn get_states(&mut self, now: Instant) -> Reply<Vec<State>> {
        let (responder, rx) = oneshot::channel();
        self.handle_command(now, Command::Request { request: protocol::get_states(), responder });
        Reply::new(rx, |raw: Box<RawValue>| parse_states(&raw))
    }

    /// Fetches the state of one entity; [`Error::Ha`] with code `not_found` if it does not exist.
    pub fn get_state(&mut self, now: Instant, entity_id: &str) -> Reply<State> {
        let (responder, rx) = oneshot::channel();
        self.handle_command(now, Command::GetStateOnce { entity_id: entity_id.to_string(), responder });
        Reply::new(rx, Ok)
    }

    /// Fetches the services of all domains (`get_services`) as JSON.
    pub fn get_services(&mut self, now: Instant) -> Reply<Value> {
        let (responder, rx) = oneshot::channel();
        self.handle_command(now, Command::Request { request: serde_json::json!({"type": "get_services"}), responder });
        Reply::new(rx, |raw: Box<RawValue>| Ok(serde_json::from_str(raw.get())?))
    }

    /// Calls a Home Assistant service; see `HAClient::call_service` for the arguments.
    /// [`Error::InvalidInput`] if `data` is not a JSON object (or `()`).
    pub fn call_service(
        &mut self,
        now: Instant,
        domain: impl Into<Domain>,
        service: impl Into<Service>,
        target: impl Into<Target>,
        data: impl Serialize,
    ) -> Result<Reply<()>> {
        let request = protocol::service_request(&domain.into(), &service.into(), &target.into(), data, false)?;
        let (responder, rx) = oneshot::channel();
        self.handle_command(now, Command::Request { request, responder });
        Ok(Reply::new(rx, |_: Box<RawValue>| Ok(())))
    }

    /// Subscribes to `source`. Works while offline too: the subscription is accepted at once,
    /// starts `Interrupted` and is sent after the next connection. See [`PendingSubscription`].
    pub fn subscribe<T>(&mut self, now: Instant, source: Source<T>) -> PendingSubscription<T> {
        let Source { kind, request, convert } = source;
        let (responder, rx) = oneshot::channel();
        self.handle_command(now, Command::Subscribe { kind, request, delivery: Delivery::Stream, responder });
        PendingSubscription { rx, convert, done: false }
    }

    /// Cancels a subscription. Events already queued are still delivered, followed by
    /// [`SubscriptionStatus::Unsubscribed`]. [`Error::InvalidInput`] for a subscription of
    /// another session.
    pub fn unsubscribe(&mut self, now: Instant, subscription: &Subscription) -> Result<Reply<()>> {
        if subscription.client != self.client_id {
            return Err(Error::InvalidInput(format!("subscription {} belongs to another client", subscription.id)));
        }
        let (responder, rx) = oneshot::channel();
        self.handle_command(now, Command::Unsubscribe { id: subscription.id, responder });
        Ok(Reply::new(rx, |_: Value| Ok(())))
    }
}

impl SubEntry {
    /// Queues an event, tracking `last_state` for entity subscriptions. A state change
    /// that `last_state` already includes is skipped: the same update (seen by both the
    /// state re-read and the trigger) or, with `or_older`, an earlier one.
    fn push_entity_event(&mut self, event: SubEvent, or_older: bool) {
        if let (SubKind::EntityState { .. }, SubEvent::StateChanged(StateChange { new_state, .. })) = (&self.kind, &event) {
            if covered(self.last_state.as_ref().and_then(Option::as_ref), new_state.as_ref(), or_older) {
                return;
            }
            self.last_state = Some(new_state.clone());
        }
        self.events.push(event);
    }
}

/// Parses a `get_states` result, skipping entities that do not parse.
pub(crate) fn parse_states(result: &RawValue) -> Result<Vec<State>> {
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

/// Oldest supported Home Assistant release: triggers use the `trigger` key (2024.10).
const MIN_HA_VERSION: (u32, u32) = (2024, 10);

/// Whether a version like `2026.9.4` (or `2024.10.0b1`) is supported. Unparsable
/// versions (dev builds) are assumed to be.
fn is_supported(version: &str) -> bool {
    let mut parts = version.split('.').map(|p| p.parse::<u32>());
    match (parts.next(), parts.next()) {
        (Some(Ok(year)), Some(Ok(month))) => (year, month) >= MIN_HA_VERSION,
        _ => true,
    }
}

/// Timestamps are compared with 1ms tolerance because `subscribe_entities` sends them as floats.
const TIMESTAMP_TOLERANCE: chrono::TimeDelta = chrono::TimeDelta::milliseconds(1);

/// Whether the current update to `new` is already reflected by `last`: the same update or,
/// with `or_older`, any update not newer than `last`.
fn covered(last: Option<&State>, new: Option<&State>, or_older: bool) -> bool {
    let (Some(last), Some(new)) = (last, new) else { return false };
    let newer_by = new.last_updated - last.last_updated;
    if or_older {
        newer_by <= TIMESTAMP_TOLERANCE
    } else {
        last.state == new.state && newer_by.abs() <= TIMESTAMP_TOLERANCE
    }
}

/// How long a session must last for the reconnect backoff to start over: the longest
/// backoff delay, at most a minute.
fn stable_session(config: &ClientConfig) -> Duration {
    config.reconnect_max_delay.min(Duration::from_secs(60))
}

/// Housekeeping granularity: fine enough for the configured timeouts, at most 1s.
pub(crate) fn tick_period(config: &ClientConfig) -> Duration {
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
    // `RandomState` is randomly seeded per instance: a dependency-free random number,
    // unlike the clock, whose resolution can be as coarse as 100ns or 1µs.
    let random = std::hash::BuildHasher::hash_one(&std::hash::RandomState::new(), attempt);
    let jitter = 0.8 + 0.4 * (random % 1000) as f64 / 1000.0;
    base.mul_f64(jitter)
}

/// Reads `entity_id` from a `subscribe_entities` initial event (`{"a": {entity: compressed}}`).
fn read_compressed(payload: &Value, entity_id: &str) -> serde_json::Result<Option<State>> {
    payload
        .get("a")
        .and_then(|a| a.get(entity_id))
        .map(|compressed| State::from_compressed(entity_id, compressed))
        .transpose()
}

/// Whether the entity differs from the last known state (`Some(None)`: known not to
/// exist). Without a baseline (`None`) nothing is reported.
fn changed(last: Option<Option<&State>>, current: Option<&State>) -> bool {
    match last {
        None => false,
        Some(None) => current.is_some(),
        Some(Some(last)) => !covered(Some(last), current, false),
    }
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
        // Jittered: repeated delays for the same attempt differ.
        let delays: std::collections::HashSet<Duration> = (0..20).map(|_| backoff(&cfg, 3)).collect();
        assert!(delays.len() > 1);
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
        assert!(changed(Some(Some(&s("1", 1.0))), None));
        assert!(changed(Some(Some(&s("1", 1.0))), Some(&s("2", 1.0))));
        assert!(changed(Some(Some(&s("1", 1.0))), Some(&s("1", 2.0))));
        assert!(!changed(Some(Some(&s("1", 1.0000004))), Some(&s("1", 1.0))));
        // Removed, then re-added while disconnected.
        assert!(changed(Some(None), Some(&s("1", 1.0))));
        assert!(!changed(Some(None), None));
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

    // --- Session without a network: a scripted Home Assistant ---

    /// Drives a `Session` the way a host would, with a manual clock and a fake server.
    struct Host {
        session: Session,
        now: Instant,
    }

    impl Host {
        fn new() -> Self {
            let config = ClientConfig::for_session("TOKEN").reconnect_delay(Duration::from_secs(1), Duration::from_secs(1));
            Self { session: Session::new(config), now: Instant::now() }
        }

        fn advance(&mut self, by: Duration) {
            self.now += by;
            self.session.tick(self.now);
        }

        /// Everything the session wants to write, as JSON.
        fn sent(&mut self) -> Vec<Value> {
            std::iter::from_fn(|| self.session.poll_transmit()).map(|f| serde_json::from_str(&f).unwrap()).collect()
        }

        fn recv(&mut self, frame: Value) {
            self.session.on_frame(self.now, &frame.to_string());
        }

        /// Connect + authenticate.
        fn connect(&mut self) {
            assert_eq!(self.session.poll_action(self.now), Some(Action::Connect));
            assert_eq!(self.session.poll_action(self.now), None);
            self.session.on_connected(self.now);
            assert!(self.sent().is_empty(), "the server speaks first");
            self.recv(json!({"type": "auth_required", "ha_version": "2026.10.0"}));
            assert_eq!(self.sent(), [json!({"type": "auth", "access_token": "TOKEN"})]);
            self.recv(json!({"type": "auth_ok", "ha_version": "2026.10.0"}));
            assert_eq!(self.session.connection_state(), ConnectionState::Connected);
        }

        fn result(&mut self, id: u64, result: Value) {
            self.recv(json!({"id": id, "type": "result", "success": true, "result": result}));
        }
    }

    fn compressed_state(entity_id: &str, state: &str, updated: f64) -> Value {
        json!({"a": {entity_id: {"s": state, "a": {}, "c": "01X", "lc": updated, "lu": updated}}})
    }

    fn entity_trigger_event(entity_id: &str, from: &str, to: &str) -> Value {
        let state = |s: &str| json!({
            "entity_id": entity_id, "state": s, "attributes": {},
            "last_changed": "2026-10-28T14:45:09.185007+00:00",
            "last_reported": "2026-10-28T14:45:09.185007+00:00",
            "last_updated": "2026-10-28T14:45:09.185007+00:00",
            "context": {"id": "01X", "parent_id": null, "user_id": null}
        });
        json!({"variables": {"trigger": {
            "id": "0", "idx": "0", "alias": null, "platform": "state", "entity_id": entity_id,
            "from_state": state(from), "to_state": state(to), "for": null, "attribute": null,
        }}, "context": {"id": "01X", "parent_id": null, "user_id": null}})
    }

    fn describe(update: Update<StateChange>) -> String {
        match update {
            Update::Event(change) => format!("{}->{}", change.old_state.map_or("-".into(), |s| s.state), change.new_state.map_or("-".into(), |s| s.state)),
            Update::Status { status, .. } => format!("{status:?}"),
        }
    }

    #[test]
    fn handshake_request_and_result() {
        let mut host = Host::new();
        assert_eq!(host.session.connection_state(), ConnectionState::Connecting);
        host.connect();

        let mut states = host.session.get_states(host.now);
        assert_eq!(host.sent(), [json!({"id": 1, "type": "get_states"})]);
        assert!(states.poll().is_none());
        host.result(1, json!([
            {"entity_id": "light.a", "state": "on", "attributes": {},
             "last_changed": "2026-10-28T14:45:09+00:00", "last_updated": "2026-10-28T14:45:09+00:00",
             "context": {"id": "01X", "parent_id": null, "user_id": null}}
        ]));
        let states = states.poll().unwrap().unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].entity_id, "light.a");

        let mut call = host.session.call_service(host.now, "light", "turn_on", "light.a", json!({"brightness": 5})).unwrap();
        assert_eq!(host.sent(), [json!({
            "id": 2, "type": "call_service", "domain": "light", "service": "turn_on",
            "service_data": {"brightness": 5}, "target": {"entity_id": ["light.a"]}
        })]);
        host.recv(json!({"id": 2, "type": "result", "success": false,
            "error": {"code": "not_found", "message": "nope"}}));
        assert!(matches!(call.poll(), Some(Err(Error::Ha { code: HaErrorCode::NotFound, .. }))));
        assert!(call.poll().is_none(), "a reply yields its result once");
    }

    #[test]
    fn requests_fail_while_offline_and_on_timeout() {
        let mut host = Host::new();
        let mut early = host.session.get_states(host.now);
        assert!(matches!(early.poll(), Some(Err(Error::NotConnected))));

        host.connect();
        let mut slow = host.session.get_states(host.now);
        host.sent();
        assert!(slow.poll().is_none());
        host.advance(Duration::from_secs(31));
        assert!(matches!(slow.poll(), Some(Err(Error::Timeout))));
    }

    #[test]
    fn subscription_gets_state_changes_and_is_restored_after_reconnect() {
        let mut host = Host::new();
        host.connect();

        let mut pending = host.session.subscribe(host.now, Source::entity("light.a"));
        let sent = host.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0]["id"], 1);
        assert_eq!(sent[0]["type"], "subscribe_trigger");
        assert!(pending.poll().is_none());

        // Acknowledged: the baseline is read before the caller learns of the subscription.
        host.result(1, Value::Null);
        assert_eq!(host.sent(), [json!({"id": 2, "type": "subscribe_entities", "entity_ids": ["light.a"]})]);
        assert!(pending.poll().is_none());
        host.result(2, Value::Null);
        host.recv(json!({"id": 2, "type": "event", "event": compressed_state("light.a", "off", 1.0)}));
        assert_eq!(host.sent(), [json!({"id": 3, "type": "unsubscribe_events", "subscription": 2})]);
        host.result(3, Value::Null);
        let mut events = pending.poll().unwrap().unwrap();

        host.recv(json!({"id": 1, "type": "event", "event": entity_trigger_event("light.a", "off", "on")}));
        assert_eq!(events.drain().map(describe).collect::<Vec<_>>(), ["off->on"]);

        // The connection drops: the subscription learns of it, requests fail.
        let mut inflight = host.session.get_states(host.now);
        host.sent();
        host.session.on_disconnected(host.now, "test");
        assert!(matches!(inflight.poll(), Some(Err(Error::ConnectionLost))));
        assert_eq!(events.drain().map(describe).collect::<Vec<_>>(), ["Interrupted"]);
        assert_eq!(host.session.connection_state(), ConnectionState::Reconnecting { attempt: 1 });
        assert_eq!(host.session.poll_action(host.now), None, "backoff first");
        host.now += Duration::from_secs(2);
        host.connect();

        // Ids restart; everything is sent again, in order.
        let sent = host.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!((sent[0]["id"].clone(), sent[0]["type"].clone()), (json!(1), json!("subscribe_trigger")));
        assert!(events.drain().next().is_none(), "not restored before the state is re-read");
        host.result(1, Value::Null);
        assert_eq!(host.sent(), [json!({"id": 2, "type": "subscribe_entities", "entity_ids": ["light.a"]})]);
        host.result(2, Value::Null);
        // It changed while we were away.
        host.recv(json!({"id": 2, "type": "event", "event": compressed_state("light.a", "off", 5.0)}));
        assert_eq!(host.sent(), [json!({"id": 3, "type": "unsubscribe_events", "subscription": 2})]);
        assert_eq!(events.drain().map(describe).collect::<Vec<_>>(), ["on->off", "Restored"]);

        // Unsubscribing ends the stream with its last status.
        let subscription = events.subscription().clone();
        let mut reply = host.session.unsubscribe(host.now, &subscription).unwrap();
        assert_eq!(host.sent(), [json!({"id": 4, "type": "unsubscribe_events", "subscription": 1})]);
        host.result(4, Value::Null);
        assert!(matches!(reply.poll(), Some(Ok(()))));
        assert_eq!(events.drain().map(describe).collect::<Vec<_>>(), ["Unsubscribed"]);
        assert!(events.is_finished());
    }

    #[test]
    fn subscribing_offline_starts_interrupted_and_a_rejected_restore_is_reported() {
        let mut host = Host::new();
        let mut pending = host.session.subscribe(host.now, Source::state_changes());
        let mut events = pending.poll().unwrap().unwrap();
        assert_eq!(events.drain().map(describe).collect::<Vec<_>>(), ["Interrupted"]);

        host.connect();
        assert_eq!(host.sent()[0]["type"], "subscribe_events");
        host.recv(json!({"id": 1, "type": "result", "success": false,
            "error": {"code": "invalid_format", "message": "bad"}}));
        let statuses: Vec<_> = events.drain().map(describe).collect();
        assert_eq!(statuses.len(), 1);
        assert!(statuses[0].starts_with("Rejected"), "{statuses:?}");
        assert!(events.is_finished());
    }

    #[test]
    fn invalid_token_is_terminal() {
        let mut host = Host::new();
        let mut pending = host.session.subscribe(host.now, Source::state_changes());
        let mut events = pending.poll().unwrap().unwrap();
        events.drain().for_each(drop);

        assert_eq!(host.session.poll_action(host.now), Some(Action::Connect));
        host.session.on_connected(host.now);
        host.recv(json!({"type": "auth_required"}));
        host.sent();
        host.recv(json!({"type": "auth_invalid", "message": "Invalid access token"}));
        assert_eq!(host.session.connection_state(), ConnectionState::AuthFailed("Invalid access token".into()));
        assert_eq!(host.session.poll_action(host.now), Some(Action::Close));
        host.now += Duration::from_secs(3600);
        assert_eq!(host.session.poll_action(host.now), None, "never retried");
        assert_eq!(events.drain().map(describe).collect::<Vec<_>>(), ["Closed"]);
        let mut late = host.session.get_states(host.now);
        assert!(matches!(late.poll(), Some(Err(Error::Stopped))));
    }

    #[test]
    fn keep_alive_and_missing_pong() {
        let mut host = Host::new();
        host.connect();
        host.advance(Duration::from_secs(29));
        assert!(host.sent().is_empty());
        host.advance(Duration::from_secs(2));
        assert_eq!(host.sent(), [json!({"id": 1, "type": "ping"})]);
        host.recv(json!({"id": 1, "type": "pong"}));
        host.advance(Duration::from_secs(1));
        assert!(host.sent().is_empty());

        // The next ping is never answered: the session ends the connection itself.
        host.advance(Duration::from_secs(30));
        assert_eq!(host.sent(), [json!({"id": 2, "type": "ping"})]);
        host.advance(Duration::from_secs(11));
        assert_eq!(host.session.poll_action(host.now), Some(Action::Close));
        assert_eq!(host.session.connection_state(), ConnectionState::Reconnecting { attempt: 1 });
    }

    #[test]
    fn failed_attempts_back_off_and_a_slow_handshake_times_out() {
        let mut host = Host::new();
        assert_eq!(host.session.poll_action(host.now), Some(Action::Connect));
        host.session.on_disconnected(host.now, "refused");
        assert_eq!(host.session.connection_state(), ConnectionState::Reconnecting { attempt: 1 });
        assert_eq!(host.session.poll_action(host.now), None);
        assert!(host.session.next_wakeup().is_some());

        host.now += Duration::from_secs(2);
        assert_eq!(host.session.poll_action(host.now), Some(Action::Connect));
        host.session.on_connected(host.now);
        // The server never sends `auth_required`.
        host.advance(Duration::from_secs(11));
        assert_eq!(host.session.poll_action(host.now), Some(Action::Close));
        assert_eq!(host.session.connection_state(), ConnectionState::Reconnecting { attempt: 2 });
        // A repeated report of the same failure changes nothing.
        host.session.on_disconnected(host.now, "closed");
        assert_eq!(host.session.connection_state(), ConnectionState::Reconnecting { attempt: 2 });
    }

    #[test]
    fn stop_closes_subscriptions_and_fails_requests() {
        let mut host = Host::new();
        host.connect();
        let mut pending = host.session.subscribe(host.now, Source::state_changes());
        host.sent();
        host.result(1, Value::Null);
        let mut events = pending.poll().unwrap().unwrap();
        let mut inflight = host.session.get_states(host.now);
        host.session.stop(host.now);
        assert!(matches!(inflight.poll(), Some(Err(Error::Stopped))));
        assert_eq!(events.drain().map(describe).collect::<Vec<_>>(), ["Closed"]);
        assert_eq!(host.session.connection_state(), ConnectionState::Stopped);
        assert_eq!(host.session.poll_action(host.now), Some(Action::Close));
        assert!(host.sent().is_empty());
    }

    #[test]
    fn version_check() {
        assert!(is_supported("2026.9.4"));
        assert!(is_supported("2024.10.0"));
        assert!(!is_supported("2024.9.3"));
        assert!(!is_supported("2023.12.1"));
        assert!(is_supported("dev"));
    }
}
