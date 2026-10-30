use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;
use tokio_tungstenite::tungstenite::Message;

use crate::config::ClientConfig;
use crate::connection::{self, WsStream};
use crate::error::Result;
use crate::queue::QueueRx;
use crate::session::{tick_period, Action, ConnectionState, Session};
use crate::subscription::{Command, EventCallback};
use crate::HAClient;

/// State shared between the runner and every [`HAClient`] handle.
pub(crate) struct Shared {
    /// Unique per client, so that a [`Subscription`](crate::Subscription) of another client is recognised.
    pub(crate) client_id: u64,
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

/// The current time for the sans-IO [`Session`]. Goes through Tokio's clock, so paused
/// time in tests moves the session too.
fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}

/// How an online phase ended.
enum Ending {
    /// Shutdown was requested, or every handle is gone.
    Stop,
    /// The connection ended; the session has scheduled the reconnect (or is terminal).
    Lost,
}

/// Drives a [`Session`] with a Tokio WebSocket: opens the socket when the session asks to,
/// feeds it frames, commands and ticks, and writes out what it queues. All protocol logic
/// lives in the session; this is the transport.
pub(crate) struct Runner {
    session: Session,
    shared: Arc<Shared>,
    /// Last state sent to `state_tx`, so that only changes are published.
    published: ConnectionState,
}

impl Drop for Runner {
    fn drop(&mut self) {
        // Reached without `stop` only if the task panicked: make sure `closed()`/`stop()`
        // waiters are released and new requests fail with `Stopped`.
        if !self.shared.state_tx.borrow().is_terminal() {
            log::error!("Home Assistant client task ended unexpectedly.");
            self.shared.state_tx.send_replace(ConnectionState::Stopped);
        }
    }
}

impl Runner {
    pub(crate) fn new(session: Session, shared: Arc<Shared>) -> Self {
        let published = session.connection_state();
        Self { session, shared, published }
    }

    fn publish(&mut self) {
        let state = self.session.connection_state();
        if state != self.published {
            self.shared.state_tx.send_replace(state.clone());
            self.published = state;
        }
    }

    /// Runs until stopped. `ws` is the connection of `HAClient::connect`; without one (see
    /// `HAClient::connect_lazy`) the session starts with an immediate connect attempt.
    pub(crate) async fn run(
        mut self,
        mut ws: Option<WsStream>,
        mut cmd_rx: mpsc::Receiver<Command>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        loop {
            let mut stream = match ws.take() {
                Some(stream) => stream,
                None => match self.connect(&mut cmd_rx, &mut shutdown_rx).await {
                    Some(stream) => stream,
                    None => break,
                },
            };
            match self.online(&mut stream, &mut cmd_rx, &mut shutdown_rx).await {
                Ending::Stop => {
                    let _ = tokio::time::timeout(Duration::from_secs(2), stream.close(None)).await;
                    break;
                }
                Ending::Lost => {}
            }
        }
        // A deliberate stop reports `Closed` to the subscriptions, not `Interrupted`. After a
        // terminal `AuthFailed` this does nothing.
        self.session.stop(now());
        self.publish();
    }

    /// Waits for the session to want a connection and opens it; commands are answered
    /// meanwhile. `None` means stop (or a terminal state).
    async fn connect(
        &mut self,
        cmd_rx: &mut mpsc::Receiver<Command>,
        shutdown_rx: &mut watch::Receiver<bool>,
    ) -> Option<WsStream> {
        loop {
            self.publish();
            if self.session.connection_state().is_terminal() {
                return None;
            }
            match self.session.poll_action(now()) {
                // Nothing to close: the socket is already gone.
                Some(Action::Close) => continue,
                Some(Action::Connect) => {
                    let config = self.session.config().clone();
                    match self.offline(connection::open(&config), cmd_rx, shutdown_rx).await? {
                        Ok(ws) => {
                            self.session.on_connected(now());
                            return Some(ws);
                        }
                        Err(e) => self.session.on_disconnected(now(), &e.to_string()),
                    }
                }
                None => {
                    let wake = self.session.next_wakeup().unwrap_or_else(|| now() + Duration::from_millis(10));
                    let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(wake));
                    self.offline(sleep, cmd_rx, shutdown_rx).await?;
                }
            }
        }
    }

    /// Drives `fut` while the session answers commands as when disconnected.
    /// Returns `None` on shutdown or when all handles are dropped.
    async fn offline<F: std::future::Future>(
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
                    Some(cmd) => self.session.handle_command(now(), cmd),
                },
                out = &mut fut => return Some(out),
            }
        }
    }

    /// Handshake and session over one open socket, until the connection ends.
    async fn online(
        &mut self,
        ws: &mut WsStream,
        cmd_rx: &mut mpsc::Receiver<Command>,
        shutdown_rx: &mut watch::Receiver<bool>,
    ) -> Ending {
        let mut tick = tokio::time::interval(tick_period(self.session.config()));
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            self.publish();
            if !self.session.has_transport() {
                return Ending::Lost;
            }
            if self.flush(ws).await.is_err() {
                return Ending::Lost;
            }
            // Not `biased`: a steady stream of frames must not starve commands and housekeeping.
            tokio::select! {
                _ = stopped(shutdown_rx) => return Ending::Stop,
                msg = ws.next() => match msg {
                    Some(Ok(Message::Text(text))) => self.session.on_frame(now(), &text),
                    Some(Ok(Message::Close(frame))) => {
                        self.session.on_disconnected(now(), &format!("server closed the connection ({frame:?})"));
                    }
                    Some(Ok(_)) => self.session.on_activity(now()),
                    Some(Err(e)) => self.session.on_disconnected(now(), &e.to_string()),
                    None => self.session.on_disconnected(now(), "connection closed"),
                },
                cmd = cmd_rx.recv() => match cmd {
                    None => return Ending::Stop,
                    Some(cmd) => self.session.handle_command(now(), cmd),
                },
                _ = tick.tick() => self.session.tick(now()),
            }
        }
    }

    /// Writes what the session queued. A write that does not complete within `pong_timeout`
    /// (e.g. a half-open connection with a full send buffer) ends the connection, instead
    /// of blocking the runner until the OS gives up on the socket.
    async fn flush(&mut self, ws: &mut WsStream) -> std::result::Result<(), ()> {
        let timeout = self.session.config().pong_timeout;
        while let Some(frame) = self.session.poll_transmit() {
            let reason = match tokio::time::timeout(timeout, ws.send(Message::Text(frame.into()))).await {
                Ok(Ok(())) => continue,
                Ok(Err(e)) => format!("failed to send: {e}"),
                Err(_) => format!("write did not complete within {timeout:?}"),
            };
            self.session.on_disconnected(now(), &reason);
            return Err(());
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
pub(crate) struct Worker {
    rx: QueueRx,
    callback: EventCallback,
    /// Weak so that queued events don't keep the client alive.
    weak_tx: mpsc::WeakSender<Command>,
    shared: Arc<Shared>,
}

impl Worker {
    /// Counted in `Shared::workers` until dropped.
    pub(crate) fn new(rx: QueueRx, callback: EventCallback, weak_tx: mpsc::WeakSender<Command>, shared: Arc<Shared>) -> Self {
        shared.workers.send_modify(|n| *n += 1);
        Self { rx, callback, weak_tx, shared }
    }

    pub(crate) async fn run(self) {
        let sub_id = self.rx.sub_id();
        while let Some(event) = self.rx.recv().await {
            // All handles dropped: the client is shutting down; drain silently.
            let Some(cmd_tx) = self.weak_tx.upgrade() else { continue };
            let client = HAClient { cmd_tx, shared: self.shared.clone() };
            // Each callback runs in its own task, so a panic is contained and logged.
            let task = tokio::spawn(IN_CALLBACK.scope((), (self.callback)(client, event, sub_id)));
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
        let started = tokio::time::Instant::now();
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
