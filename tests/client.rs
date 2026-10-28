//! End-to-end tests against a scripted mock of the Home Assistant WebSocket API.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use ha_core_client::events::Event;
use ha_core_client::state::{Domain, Service};
use ha_core_client::triggers::{TemplateTrigger, TimePatternTrigger};
use ha_core_client::{CallbackResult, ClientConfig, ConnectionState, EntityChanges, Error, HAClient, SubscriptionStatus};
use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const TOKEN: &str = "test-token";
const WAIT: Duration = Duration::from_secs(5);

struct MockHa {
    listener: TcpListener,
    url: String,
}

impl MockHa {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/api/websocket", listener.local_addr().unwrap());
        Self { listener, url }
    }

    fn config(&self) -> ClientConfig {
        ClientConfig::new(&self.url, TOKEN).reconnect_delay(Duration::from_millis(10), Duration::from_millis(50))
    }

    /// Accepts a connection and reads the client's `auth` message.
    async fn accept_unauthed(&self) -> Conn {
        let (tcp, _) = tokio::time::timeout(WAIT, self.listener.accept()).await.expect("no connection").unwrap();
        let mut conn = Conn { ws: tokio_tungstenite::accept_async(tcp).await.unwrap(), last_id: 0 };
        conn.send(json!({"type": "auth_required", "ha_version": "2026.9.4"})).await;
        let auth = conn.recv().await;
        assert_eq!(auth, json!({"type": "auth", "access_token": TOKEN}));
        conn
    }

    async fn accept(&self) -> Conn {
        let mut conn = self.accept_unauthed().await;
        conn.send(json!({"type": "auth_ok", "ha_version": "2026.9.4"})).await;
        conn
    }

    async fn connect(&self, config: ClientConfig) -> (HAClient, Conn) {
        let (client, conn) = tokio::join!(HAClient::connect_with(config), self.accept());
        (client.unwrap(), conn)
    }
}

struct Conn {
    ws: WebSocketStream<TcpStream>,
    last_id: u64,
}

impl Conn {
    /// Next JSON message from the client. Asserts that ids strictly increase,
    /// as Home Assistant requires.
    async fn recv(&mut self) -> Value {
        loop {
            let msg = tokio::time::timeout(WAIT, self.ws.next()).await.expect("client sent nothing");
            match msg.expect("connection closed").unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if let Some(id) = v["id"].as_u64() {
                        assert!(id > self.last_id, "id {id} not greater than {}", self.last_id);
                        self.last_id = id;
                    }
                    return v;
                }
                Message::Close(_) => panic!("client closed the connection"),
                _ => continue,
            }
        }
    }

    async fn recv_type(&mut self, msg_type: &str) -> Value {
        let v = self.recv().await;
        assert_eq!(v["type"], msg_type, "unexpected message {v}");
        v
    }

    async fn send(&mut self, v: Value) {
        self.ws.send(Message::Text(v.to_string().into())).await.unwrap();
    }

    async fn ok(&mut self, id: &Value, result: Value) {
        self.send(json!({"id": id, "type": "result", "success": true, "result": result})).await;
    }

    async fn event(&mut self, id: &Value, event: Value) {
        self.send(json!({"id": id, "type": "event", "event": event})).await;
    }

    /// Waits for the client to close the connection.
    async fn expect_close(&mut self) {
        loop {
            match tokio::time::timeout(WAIT, self.ws.next()).await.expect("client did not close") {
                None | Some(Ok(Message::Close(_))) | Some(Err(_)) => return,
                Some(Ok(_)) => continue,
            }
        }
    }
}

fn state(entity_id: &str, state: &str) -> Value {
    json!({
        "entity_id": entity_id, "state": state, "attributes": {"friendly_name": "Test"},
        "last_changed": "2026-10-28T14:45:09.185007+00:00",
        "last_reported": "2026-10-28T14:45:09.185007+00:00",
        "last_updated": "2026-10-28T14:45:09.185007+00:00",
        "context": {"id": "01X", "parent_id": null, "user_id": null}
    })
}

fn entity_trigger_event(entity_id: &str, from: &str, to: &str) -> Value {
    json!({
        "variables": {"trigger": {
            "id": "0", "idx": "0", "alias": null, "platform": "state", "entity_id": entity_id,
            "from_state": state(entity_id, from), "to_state": state(entity_id, to),
            "for": null, "attribute": null, "description": format!("state of {entity_id}")
        }},
        "context": {"id": "01X", "parent_id": null, "user_id": null}
    })
}

/// Describes an event as a short string: `"old->new"` for state changes, the status otherwise.
fn describe(event: &Event) -> String {
    match event {
        Event::StateChanged { old_state, new_state, .. } => {
            let s = |st: &Option<ha_core_client::events::State>| st.as_ref().map_or("-".to_string(), |s| s.state.clone());
            format!("{}->{}", s(old_state), s(new_state))
        }
        Event::Status { status, .. } => format!("{status:?}"),
        Event::TriggerFired(t) => format!("trigger {}", t.trigger["n"]),
        other => format!("{other:?}"),
    }
}

/// A callback forwarding `describe(event)` to a channel.
fn collector() -> (
    impl Fn(HAClient, Event) -> std::future::Ready<CallbackResult> + Send + Sync + 'static,
    mpsc::UnboundedReceiver<String>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    (move |_, event: Event| std::future::ready(tx.send(describe(&event)).map_err(Into::into)), rx)
}

/// Answers a one-shot `subscribe_entities` read (`msg`) of `entity_id`.
async fn answer_read(conn: &mut Conn, msg: &Value, entity_id: &str, state: &str, last_updated: f64) {
    assert_eq!(msg["entity_ids"], json!([entity_id]));
    conn.ok(&msg["id"], Value::Null).await;
    conn.event(&msg["id"], json!({"a": {entity_id: {"s": state, "a": {}, "c": "01X", "lc": last_updated, "lu": last_updated}}}))
        .await;
}

/// Receives the client's one-shot read of `entity_id`, answers it, and expects the cleanup.
async fn answer_state_read(conn: &mut Conn, entity_id: &str, state: &str, last_updated: f64) {
    let msg = conn.recv_type("subscribe_entities").await;
    answer_read(conn, &msg, entity_id, state, last_updated).await;
    let unsub = conn.recv_type("unsubscribe_events").await;
    assert_eq!(unsub["subscription"], msg["id"]);
}

/// Closes the server side of `conn` and returns the client's next, fully connected session.
async fn reconnect(ha: &MockHa, client: &HAClient, mut conn: Conn) -> Conn {
    conn.ws.close(None).await.unwrap();
    wait_state(client, |s| matches!(s, ConnectionState::Reconnecting { .. })).await;
    let conn = ha.accept().await;
    wait_state(client, |s| *s == ConnectionState::Connected).await;
    conn
}

/// Subscribes to `entity_id`, acknowledges on the mock side and answers the baseline
/// state read with `state` (`"1"`, last updated at t=100).
async fn subscribe_collecting(client: &HAClient, conn: &mut Conn, entity_id: &str) -> (mpsc::UnboundedReceiver<String>, Value) {
    let (callback, rx) = collector();
    let sub = {
        let client = client.clone();
        let entity_id = entity_id.to_string();
        tokio::spawn(async move { client.subscribe_entity(&entity_id, callback).await })
    };
    let msg = conn.recv_type("subscribe_trigger").await;
    assert_eq!(msg["trigger"], json!({"trigger": "state", "entity_id": entity_id}));
    conn.ok(&msg["id"], Value::Null).await;
    // `subscribe_entity` completes only once the entity is known to exist.
    answer_state_read(conn, entity_id, "1", 100.0).await;
    sub.await.unwrap().unwrap();
    (rx, msg["id"].clone())
}

/// Subscribes to a trigger and acknowledges it on the mock side.
async fn subscribe_trigger_collecting(client: &HAClient, conn: &mut Conn) -> mpsc::UnboundedReceiver<String> {
    let (callback, rx) = collector();
    let sub = {
        let client = client.clone();
        tokio::spawn(async move { client.subscribe_trigger(TimePatternTrigger::new().seconds("/5"), callback).await })
    };
    let msg = conn.recv_type("subscribe_trigger").await;
    conn.ok(&msg["id"], Value::Null).await;
    sub.await.unwrap().unwrap();
    rx
}

/// Sends `get_states` and answers it, asserting it is the next message the client sends.
async fn round_trip(client: &HAClient, conn: &mut Conn) {
    let req = tokio::spawn({
        let client = client.clone();
        async move { client.get_states().await }
    });
    let msg = conn.recv_type("get_states").await;
    conn.ok(&msg["id"], json!([])).await;
    req.await.unwrap().unwrap();
}

async fn wait_state(client: &HAClient, pred: impl Fn(&ConnectionState) -> bool) {
    let mut rx = client.watch_connection_state();
    tokio::time::timeout(WAIT, rx.wait_for(|s| pred(s))).await.expect("state not reached").unwrap();
}

async fn recv_within<T>(rx: &mut mpsc::UnboundedReceiver<T>) -> T {
    tokio::time::timeout(WAIT, rx.recv()).await.expect("nothing received").unwrap()
}

#[tokio::test]
async fn invalid_token_is_reported_by_connect() {
    let ha = MockHa::start().await;
    let server = async {
        let mut conn = ha.accept_unauthed().await;
        conn.send(json!({"type": "auth_invalid", "message": "Invalid access token or password"})).await;
    };
    let (result, _) = tokio::join!(HAClient::connect_with(ha.config()), server);
    assert!(matches!(result, Err(Error::Auth(m)) if m.contains("Invalid access token")));
}

#[tokio::test]
async fn trigger_events_are_delivered_in_order() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;

    // The callback sleeps longer for earlier events; order must still be preserved.
    // (Trigger events are never merged, unlike entity state changes.)
    let (tx, mut rx) = mpsc::unbounded_channel();
    let sub = {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .subscribe_trigger(TimePatternTrigger::new().seconds("/5"), move |_, event| {
                    let tx = tx.clone();
                    async move {
                        let Event::TriggerFired(t) = event else {
                            return Err(format!("unexpected event {event:?}").into());
                        };
                        let n = t.trigger["n"].as_u64().unwrap();
                        tokio::time::sleep(Duration::from_millis(30 / n)).await;
                        tx.send(n).unwrap();
                        Ok(())
                    }
                })
                .await
        })
    };
    let msg = conn.recv_type("subscribe_trigger").await;
    conn.ok(&msg["id"], Value::Null).await;
    sub.await.unwrap().unwrap();

    for n in 1..=3 {
        conn.event(&msg["id"], time_pattern_event(n)).await;
    }
    for n in 1..=3 {
        assert_eq!(recv_within(&mut rx).await, n);
    }
}

#[tokio::test]
async fn rejected_subscription_returns_ha_error() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;

    let sub = {
        let client = client.clone();
        tokio::spawn(async move {
            client.subscribe_trigger(TemplateTrigger::new("{{ invalid"), |_, _| async { Ok(()) }).await
        })
    };
    let msg = conn.recv_type("subscribe_trigger").await;
    assert_eq!(msg["trigger"]["trigger"], "template");
    conn.send(json!({"id": msg["id"], "type": "result", "success": false,
        "error": {"code": "invalid_format", "message": "invalid template"}}))
        .await;
    let err = sub.await.unwrap().unwrap_err();
    assert!(matches!(err, Error::Ha { ref code, .. } if code == "invalid_format"), "{err:?}");
}

#[tokio::test]
async fn reconnect_after_server_close_restores_subscriptions() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let (mut rx_a, _) = subscribe_collecting(&client, &mut conn, "sensor.a").await;
    let (mut rx_b, _) = subscribe_collecting(&client, &mut conn, "sensor.b").await;

    // Home Assistant restarting: a clean close frame.
    conn.ws.close(None).await.unwrap();
    drop(conn);
    assert_eq!(recv_within(&mut rx_a).await, "Interrupted");
    assert_eq!(recv_within(&mut rx_b).await, "Interrupted");
    wait_state(&client, |s| matches!(s, ConnectionState::Reconnecting { .. })).await;

    let mut conn = ha.accept().await;
    // Fresh, increasing ids on the new connection, in creation order.
    let first = conn.recv_type("subscribe_trigger").await;
    let second = conn.recv_type("subscribe_trigger").await;
    assert_eq!(first["id"], 1);
    assert_eq!(first["trigger"]["entity_id"], "sensor.a");
    assert_eq!(second["trigger"]["entity_id"], "sensor.b");
    conn.ok(&first["id"], Value::Null).await;
    conn.ok(&second["id"], Value::Null).await;
    wait_state(&client, |s| *s == ConnectionState::Connected).await;

    // The states are re-read: `a` did not change, `b` changed while disconnected.
    let read_a = conn.recv_type("subscribe_entities").await;
    let read_b = conn.recv_type("subscribe_entities").await;
    answer_read(&mut conn, &read_a, "sensor.a", "1", 100.0).await;
    answer_read(&mut conn, &read_b, "sensor.b", "2", 200.0).await;
    conn.recv_type("unsubscribe_events").await;
    conn.recv_type("unsubscribe_events").await;
    assert_eq!(recv_within(&mut rx_a).await, "Restored");
    assert_eq!(recv_within(&mut rx_b).await, "1->2");
    assert_eq!(recv_within(&mut rx_b).await, "Restored");

    conn.event(&second["id"], entity_trigger_event("sensor.b", "2", "3")).await;
    conn.event(&first["id"], entity_trigger_event("sensor.a", "1", "4")).await;
    assert_eq!(recv_within(&mut rx_b).await, "2->3");
    assert_eq!(recv_within(&mut rx_a).await, "1->4");

    // Requests work again on the new connection.
    round_trip(&client, &mut conn).await;
}

#[tokio::test]
async fn removed_entity_is_reported_after_reconnect() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let (mut rx, _) = subscribe_collecting(&client, &mut conn, "sensor.a").await;
    conn.ws.close(None).await.unwrap();
    assert_eq!(recv_within(&mut rx).await, "Interrupted");

    let mut conn = ha.accept().await;
    let sub = conn.recv_type("subscribe_trigger").await;
    conn.ok(&sub["id"], Value::Null).await;
    let read = conn.recv_type("subscribe_entities").await;
    conn.ok(&read["id"], Value::Null).await;
    conn.event(&read["id"], json!({"a": {}})).await;
    assert_eq!(recv_within(&mut rx).await, "1->-");
    assert_eq!(recv_within(&mut rx).await, "Restored");
}

#[tokio::test]
async fn trigger_subscriptions_get_interrupted_and_restored() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let mut rx = subscribe_trigger_collecting(&client, &mut conn).await;
    conn.ws.close(None).await.unwrap();
    assert_eq!(recv_within(&mut rx).await, "Interrupted");

    let mut conn = ha.accept().await;
    let sub = conn.recv_type("subscribe_trigger").await;
    conn.ok(&sub["id"], Value::Null).await;
    assert_eq!(recv_within(&mut rx).await, "Restored");
}

#[tokio::test]
async fn rejected_resubscription_is_reported_and_dropped() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let mut rx = subscribe_trigger_collecting(&client, &mut conn).await;
    conn.ws.close(None).await.unwrap();
    assert_eq!(recv_within(&mut rx).await, "Interrupted");

    let mut conn = ha.accept().await;
    let sub = conn.recv_type("subscribe_trigger").await;
    conn.send(json!({"id": sub["id"], "type": "result", "success": false,
        "error": {"code": "invalid_format", "message": "integration not loaded"}}))
        .await;
    assert_eq!(
        recv_within(&mut rx).await,
        r#"Rejected { code: "invalid_format", message: "integration not loaded" }"#
    );

    // Gone for good: not re-sent after the next reconnect either.
    wait_state(&client, |s| *s == ConnectionState::Connected).await;
    let mut conn = reconnect(&ha, &client, conn).await;
    round_trip(&client, &mut conn).await;
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn handler_can_unsubscribe_while_interrupted() {
    let ha = MockHa::start().await;
    let config = ha.config().reconnect_delay(Duration::from_millis(300), Duration::from_millis(300));
    let (client, mut conn) = ha.connect(config).await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let sub = {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .subscribe_trigger(TimePatternTrigger::new().seconds("/5"), move |client, event| {
                    let tx = tx.clone();
                    async move {
                        if let Event::Status { subscription, status: SubscriptionStatus::Interrupted } = event {
                            tx.send(client.unsubscribe(subscription).await.is_ok()).unwrap();
                        }
                        Ok(())
                    }
                })
                .await
        })
    };
    let msg = conn.recv_type("subscribe_trigger").await;
    conn.ok(&msg["id"], Value::Null).await;
    sub.await.unwrap().unwrap();

    conn.ws.close(None).await.unwrap();
    assert!(recv_within(&mut rx).await, "unsubscribe while offline failed");

    // Nothing is restored on the new connection.
    let mut conn = ha.accept().await;
    wait_state(&client, |s| *s == ConnectionState::Connected).await;
    round_trip(&client, &mut conn).await;
}

#[tokio::test]
async fn subscriptions_are_closed_on_stop() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let mut rx = subscribe_trigger_collecting(&client, &mut conn).await;
    client.stop().await;
    assert_eq!(recv_within(&mut rx).await, "Closed");
}

#[tokio::test]
async fn blocked_write_triggers_reconnect() {
    let ha = MockHa::start().await;
    let config = ha.config().pong_timeout(Duration::from_millis(300)).request_timeout(Duration::from_secs(30));
    // The mock never reads from this connection, so the client's socket buffer fills up.
    let (client, _stalled) = ha.connect(config).await;

    let payload = json!({ "blob": "x".repeat(1 << 20) });
    for _ in 0..64 {
        let client = client.clone();
        let payload = payload.clone();
        tokio::spawn(async move { client.call_service("test", "noop", None, Some(payload)).await });
    }
    wait_state(&client, |s| matches!(s, ConnectionState::Reconnecting { .. })).await;
    let mut conn = ha.accept().await;
    wait_state(&client, |s| *s == ConnectionState::Connected).await;
    round_trip(&client, &mut conn).await;
}

#[tokio::test]
async fn connection_dropped_right_after_auth_keeps_backing_off() {
    let ha = MockHa::start().await;
    // A session must last 5s (the longest delay) to count as stable.
    let config = ha.config().reconnect_delay(Duration::from_millis(10), Duration::from_secs(5));
    let (client, mut conn) = ha.connect(config).await;
    for attempt in 1..=3 {
        conn.ws.close(None).await.unwrap();
        wait_state(&client, |s| *s == ConnectionState::Reconnecting { attempt }).await;
        conn = ha.accept().await;
        wait_state(&client, |s| *s == ConnectionState::Connected).await;
    }

    // After a session that stayed up (50ms here), the next reconnect starts over.
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    for (stay_up, attempt) in [(false, 1), (false, 2), (true, 1)] {
        if stay_up {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        conn.ws.close(None).await.unwrap();
        wait_state(&client, |s| *s == ConnectionState::Reconnecting { attempt }).await;
        conn = ha.accept().await;
        wait_state(&client, |s| *s == ConnectionState::Connected).await;
    }
}

#[tokio::test]
async fn unlimited_timeouts_are_accepted() {
    let ha = MockHa::start().await;
    let config = ha.config().request_timeout(Duration::MAX).ping_interval(Duration::MAX).pong_timeout(Duration::MAX);
    let (client, mut conn) = ha.connect(config).await;
    round_trip(&client, &mut conn).await;
    assert_eq!(client.connection_state(), ConnectionState::Connected);
}

#[tokio::test]
async fn requests_fail_fast_while_disconnected() {
    let ha = MockHa::start().await;
    let config = ha.config().reconnect_delay(Duration::from_secs(30), Duration::from_secs(30));
    let (client, mut conn) = ha.connect(config).await;
    conn.ws.close(None).await.unwrap();
    wait_state(&client, |s| matches!(s, ConnectionState::Reconnecting { .. })).await;

    assert!(matches!(client.get_states().await, Err(Error::NotConnected)));
    assert!(matches!(client.turn_on("light.a").await, Err(Error::NotConnected)));
}

#[tokio::test]
async fn in_flight_request_fails_when_connection_drops() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let req = tokio::spawn({
        let client = client.clone();
        async move { client.get_states().await }
    });
    conn.recv_type("get_states").await;
    conn.ws.close(None).await.unwrap();
    assert!(matches!(req.await.unwrap(), Err(Error::ConnectionLost)));
}

#[tokio::test]
async fn unanswered_request_times_out() {
    let ha = MockHa::start().await;
    let config = ha.config().request_timeout(Duration::from_millis(300));
    let (client, mut conn) = ha.connect(config).await;
    let req = tokio::spawn({
        let client = client.clone();
        async move { client.get_states().await }
    });
    conn.recv_type("get_states").await;
    let result = tokio::time::timeout(WAIT, req).await.unwrap().unwrap();
    assert!(matches!(result, Err(Error::Timeout)), "{result:?}");
}

/// A `render_template` event as Home Assistant sends it (captured from 2026.9).
fn rendered(result: Value) -> Value {
    json!({"result": result, "listeners": {"all": false, "entities": ["sensor.outdoor"], "domains": [], "time": true}})
}

#[tokio::test]
async fn subscribe_entity_times_out_as_a_whole() {
    let ha = MockHa::start().await;
    let config = ha.config().request_timeout(Duration::from_secs(1));
    let (client, mut conn) = ha.connect(config).await;
    let sub = tokio::spawn({
        let client = client.clone();
        async move { client.subscribe_entity("sensor.a", |_, _| async { Ok(()) }).await }
    });
    // Each step answers within the request timeout (ack at 0.6s, read at 1.4s),
    // both together do not.
    let trigger = conn.recv_type("subscribe_trigger").await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    conn.ok(&trigger["id"], Value::Null).await;
    let read = conn.recv_type("subscribe_entities").await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    answer_read(&mut conn, &read, "sensor.a", "1", 100.0).await;
    let result = tokio::time::timeout(WAIT, sub).await.unwrap().unwrap();
    assert!(matches!(result, Err(Error::Timeout)), "{result:?}");
    // The trigger Home Assistant created is cancelled, and so is the late read.
    let unsub = conn.recv_type("unsubscribe_events").await;
    assert_eq!(unsub["subscription"], trigger["id"]);
    let unsub = conn.recv_type("unsubscribe_events").await;
    assert_eq!(unsub["subscription"], read["id"]);
}

#[tokio::test]
async fn template_render_before_the_ack_is_delivered() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let (callback, mut rx) = collector();
    let sub = tokio::spawn({
        let client = client.clone();
        async move { client.subscribe_template("{{ 1 + 1 }}", callback).await }
    });
    let msg = conn.recv_type("render_template").await;
    assert_eq!(msg["template"], "{{ 1 + 1 }}");
    assert_eq!(msg["report_errors"], true);
    // Home Assistant may send the first render ahead of the result.
    conn.event(&msg["id"], rendered(json!(2))).await;
    conn.ok(&msg["id"], Value::Null).await;
    sub.await.unwrap().unwrap();
    assert_eq!(recv_within(&mut rx).await, "TemplateRendered { result: Number(2) }");
    // It was not mistaken for an unknown subscription and cancelled.
    round_trip(&client, &mut conn).await;

    conn.event(&msg["id"], json!({"error": "ZeroDivisionError: division by zero", "level": "ERROR"})).await;
    assert_eq!(
        recv_within(&mut rx).await,
        r#"TemplateError { message: "ZeroDivisionError: division by zero", level: "ERROR" }"#
    );

    // After a reconnect the current result follows `Restored`.
    let mut conn = reconnect(&ha, &client, conn).await;
    let msg = conn.recv_type("render_template").await;
    conn.event(&msg["id"], rendered(json!(2))).await;
    conn.ok(&msg["id"], Value::Null).await;
    assert_eq!(recv_within(&mut rx).await, "Interrupted");
    assert_eq!(recv_within(&mut rx).await, "Restored");
    assert_eq!(recv_within(&mut rx).await, "TemplateRendered { result: Number(2) }");
}

#[tokio::test]
async fn watch_silence_reports_silent_and_back() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let invalid = client.watch_silence("sensor.x }}{{ 1", Duration::from_secs(60), collector().0).await;
    assert!(matches!(invalid, Err(Error::InvalidInput(_))), "{invalid:?}");

    let (callback, mut rx) = collector();
    let sub = tokio::spawn({
        let client = client.clone();
        async move { client.watch_silence("sensor.outdoor", Duration::from_secs(1800), callback).await }
    });
    let msg = conn.recv_type("render_template").await;
    let template = msg["template"].as_str().unwrap();
    assert!(template.contains("states.sensor['outdoor']"), "{template}");
    assert!(template.contains("last_reported).total_seconds() > 1800"), "{template}");
    conn.ok(&msg["id"], Value::Null).await;
    conn.event(&msg["id"], rendered(json!(false))).await;
    sub.await.unwrap().unwrap();
    conn.event(&msg["id"], rendered(json!(true))).await;
    conn.event(&msg["id"], rendered(json!(false))).await;
    for silent in [false, true, false] {
        assert_eq!(recv_within(&mut rx).await, format!(r#"Silence {{ entity_id: "sensor.outdoor", silent: {silent} }}"#));
    }
}

#[tokio::test]
async fn get_state_reads_compressed_state() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;

    let req = tokio::spawn({
        let client = client.clone();
        async move { client.get_state("switch.pump").await }
    });
    let msg = conn.recv_type("subscribe_entities").await;
    assert_eq!(msg["entity_ids"], json!(["switch.pump"]));
    conn.ok(&msg["id"], Value::Null).await;
    // Changed by a user, so the context is an object rather than a bare id.
    conn.event(&msg["id"], json!({"a": {"switch.pump": {
        "s": "off", "a": {"friendly_name": "Pump"},
        "c": {"id": "01M", "parent_id": null, "user_id": "abc"}, "lc": 1792706118.1
    }}}))
    .await;
    let state = req.await.unwrap().unwrap();
    assert_eq!(state.state, "off");
    assert_eq!(state.context["user_id"], "abc");
    // The one-shot subscription is cleaned up.
    let unsub = conn.recv_type("unsubscribe_events").await;
    assert_eq!(unsub["subscription"], msg["id"]);

    let req = tokio::spawn({
        let client = client.clone();
        async move { client.get_state("sensor.missing").await }
    });
    let msg = conn.recv_type("subscribe_entities").await;
    conn.ok(&msg["id"], Value::Null).await;
    conn.event(&msg["id"], json!({"a": {}})).await;
    assert!(matches!(req.await.unwrap(), Err(Error::Ha { code, .. }) if code == "not_found"));
}

#[tokio::test]
async fn service_calls_use_wire_names_and_target() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;

    let calls = tokio::spawn({
        let client = client.clone();
        async move {
            client.turn_on("fan.bedroom").await?;
            client.call_service(Domain::HomeAssistant, Service::Reload, None, None).await?;
            client.call_service_with_response("weather", "get_forecasts", Some("weather.home"), Some(json!({"type": "daily"}))).await
        }
    });

    let m = conn.recv_type("call_service").await;
    assert_eq!((m["domain"].as_str(), m["service"].as_str()), (Some("fan"), Some("turn_on")));
    assert_eq!(m["target"], json!({"entity_id": "fan.bedroom"}));
    conn.ok(&m["id"], json!({"context": {}, "response": null})).await;

    let m = conn.recv_type("call_service").await;
    assert_eq!(m["domain"], "homeassistant");
    assert!(m.get("target").is_none());
    conn.ok(&m["id"], json!({"context": {}, "response": null})).await;

    let m = conn.recv_type("call_service").await;
    assert_eq!(m["return_response"], true);
    conn.ok(&m["id"], json!({"context": {}, "response": {"weather.home": {"forecast": []}}})).await;

    let response = calls.await.unwrap().unwrap();
    assert_eq!(response, json!({"weather.home": {"forecast": []}}));

    assert!(matches!(client.turn_on("not-an-entity").await, Err(Error::InvalidInput(_))));
}

#[tokio::test]
async fn missing_pong_triggers_reconnect() {
    let ha = MockHa::start().await;
    let config = ha.config().ping_interval(Duration::from_millis(100)).pong_timeout(Duration::from_millis(200));
    let (client, mut conn) = ha.connect(config).await;

    let ping = conn.recv_type("ping").await;
    assert!(ping["id"].is_u64());
    // Never answer: the client must give up on this connection and reconnect.
    let mut conn2 = ha.accept().await;
    wait_state(&client, |s| *s == ConnectionState::Connected).await;

    // A connection that answers pings stays up.
    let ping = conn2.recv_type("ping").await;
    conn2.send(json!({"id": ping["id"], "type": "pong"})).await;
    let ping = conn2.recv_type("ping").await;
    conn2.send(json!({"id": ping["id"], "type": "pong"})).await;
    assert_eq!(client.connection_state(), ConnectionState::Connected);
    drop(conn);
}

#[tokio::test]
async fn auth_failure_on_reconnect_is_terminal() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    conn.ws.close(None).await.unwrap();

    let mut conn = ha.accept_unauthed().await;
    conn.send(json!({"type": "auth_invalid", "message": "token revoked"})).await;
    let state = tokio::time::timeout(WAIT, client.closed()).await.unwrap();
    assert_eq!(state, ConnectionState::AuthFailed("token revoked".into()));
    assert!(matches!(client.get_states().await, Err(Error::Stopped)));

    // No further attempts.
    assert!(tokio::time::timeout(Duration::from_millis(300), ha.listener.accept()).await.is_err());
}

#[tokio::test]
async fn stop_closes_the_connection() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let other_handle = client.clone();

    tokio::time::timeout(WAIT, client.stop()).await.unwrap();
    conn.expect_close().await;
    assert_eq!(other_handle.connection_state(), ConnectionState::Stopped);
    assert!(matches!(other_handle.get_states().await, Err(Error::Stopped)));
}

#[tokio::test]
async fn dropping_all_handles_stops_the_client() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    drop(client);
    conn.expect_close().await;
}

#[tokio::test]
async fn late_events_of_expired_get_state_are_unsubscribed() {
    let ha = MockHa::start().await;
    let config = ha.config().request_timeout(Duration::from_millis(200));
    let (client, mut conn) = ha.connect(config).await;

    let req = tokio::spawn({
        let client = client.clone();
        async move { client.get_state("sensor.slow").await }
    });
    let msg = conn.recv_type("subscribe_entities").await;
    assert!(matches!(req.await.unwrap(), Err(Error::Timeout)));

    // Home Assistant answers late; the client cancels the orphaned subscription once.
    conn.ok(&msg["id"], Value::Null).await;
    conn.event(&msg["id"], json!({"a": {}})).await;
    conn.event(&msg["id"], json!({"c": {}})).await;
    let unsub = conn.recv_type("unsubscribe_events").await;
    assert_eq!(unsub["subscription"], msg["id"]);

    let req = tokio::spawn({
        let client = client.clone();
        async move { client.get_states().await }
    });
    // The next message is the new request, not a second unsubscribe.
    let next = conn.recv_type("get_states").await;
    conn.ok(&next["id"], json!([])).await;
    assert!(req.await.unwrap().unwrap().is_empty());
}

type BoxedFuture = std::pin::Pin<Box<dyn std::future::Future<Output = CallbackResult> + Send>>;

/// A callback that records each event as soon as it starts, then waits for a permit,
/// so the test controls when the subscription catches up.
fn gated_collector() -> (
    impl Fn(HAClient, Event) -> BoxedFuture + Send + Sync + 'static,
    mpsc::UnboundedReceiver<String>,
    std::sync::Arc<tokio::sync::Semaphore>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    let gate = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
    let callback = {
        let gate = gate.clone();
        move |_: HAClient, event: Event| {
            let (tx, gate) = (tx.clone(), gate.clone());
            Box::pin(async move {
                tx.send(describe(&event))?;
                gate.acquire().await?.forget();
                Ok(())
            }) as BoxedFuture
        }
    };
    (callback, rx, gate)
}

fn time_pattern_event(n: u32) -> Value {
    json!({"variables": {"trigger": {"platform": "time_pattern", "n": n}}, "context": null})
}

#[tokio::test]
async fn slow_entity_callback_gets_merged_state_changes() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let (callback, mut rx, gate) = gated_collector();
    let sub = tokio::spawn({
        let client = client.clone();
        async move { client.subscribe_entity("sensor.power", callback).await }
    });
    let msg = conn.recv_type("subscribe_trigger").await;
    conn.ok(&msg["id"], Value::Null).await;
    answer_state_read(&mut conn, "sensor.power", "0", 100.0).await;
    sub.await.unwrap().unwrap();

    // The callback takes the first change and blocks on it...
    conn.event(&msg["id"], entity_trigger_event("sensor.power", "0", "1")).await;
    assert_eq!(recv_within(&mut rx).await, "0->1");
    // ...while four more arrive. Once this answer arrives, they have all been queued.
    for n in 2..=5 {
        conn.event(&msg["id"], entity_trigger_event("sensor.power", &(n - 1).to_string(), &n.to_string())).await;
    }
    round_trip(&client, &mut conn).await;

    gate.add_permits(10);
    assert_eq!(recv_within(&mut rx).await, "Lagged { dropped: 3 }");
    assert_eq!(recv_within(&mut rx).await, "1->5");
}

#[tokio::test]
async fn slow_trigger_callback_drops_oldest_events_but_not_statuses() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config().subscription_queue_size(3)).await;
    let (callback, mut rx, gate) = gated_collector();
    let sub = tokio::spawn({
        let client = client.clone();
        async move { client.subscribe_trigger(TimePatternTrigger::new().seconds("/5"), callback).await }
    });
    let msg = conn.recv_type("subscribe_trigger").await;
    conn.ok(&msg["id"], Value::Null).await;
    sub.await.unwrap().unwrap();

    conn.event(&msg["id"], time_pattern_event(1)).await;
    assert_eq!(recv_within(&mut rx).await, "trigger 1");
    for n in 2..=6 {
        conn.event(&msg["id"], time_pattern_event(n)).await;
    }
    round_trip(&client, &mut conn).await;

    // A status while the queue is full is kept.
    conn.ws.close(None).await.unwrap();
    wait_state(&client, |s| matches!(s, ConnectionState::Reconnecting { .. })).await;

    gate.add_permits(10);
    let mut got = Vec::new();
    for _ in 0..5 {
        got.push(recv_within(&mut rx).await);
    }
    assert_eq!(got, ["Lagged { dropped: 2 }", "trigger 4", "trigger 5", "trigger 6", "Interrupted"]);
}

mod slow_callback_log {
    use super::*;
    use std::sync::Mutex;

    static LINES: Mutex<Vec<String>> = Mutex::new(Vec::new());

    struct Capture;
    impl log::Log for Capture {
        fn enabled(&self, m: &log::Metadata) -> bool {
            m.level() <= log::Level::Warn
        }
        fn log(&self, r: &log::Record) {
            if self.enabled(r.metadata()) {
                LINES.lock().unwrap().push(r.args().to_string());
            }
        }
        fn flush(&self) {}
    }

    #[tokio::test]
    async fn slow_callback_is_reported() {
        let _ = log::set_logger(&Capture);
        log::set_max_level(log::LevelFilter::Warn);

        let ha = MockHa::start().await;
        let config = ha.config().slow_callback_warning(Duration::from_millis(300));
        let (client, mut conn) = ha.connect(config).await;
        let (callback, mut rx, gate) = gated_collector();
        let sub = tokio::spawn({
            let client = client.clone();
            async move { client.subscribe_trigger(TimePatternTrigger::new().seconds("/5"), callback).await }
        });
        let msg = conn.recv_type("subscribe_trigger").await;
        conn.ok(&msg["id"], Value::Null).await;
        let sub_id = sub.await.unwrap().unwrap().id();

        conn.event(&msg["id"], time_pattern_event(1)).await;
        assert_eq!(recv_within(&mut rx).await, "trigger 1");
        conn.event(&msg["id"], time_pattern_event(2)).await;
        round_trip(&client, &mut conn).await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        gate.add_permits(10);
        assert_eq!(recv_within(&mut rx).await, "trigger 2");

        let lines = LINES.lock().unwrap().clone();
        let prefix = format!("Callback of subscription {sub_id} has been running for 300ms; 1 event(s) waiting");
        assert!(lines.contains(&prefix), "{lines:?}");
        assert!(lines.iter().any(|l| l.starts_with(&format!("Slow callback of subscription {sub_id} finished after"))));
    }
}

#[tokio::test]
async fn subscribing_to_a_missing_entity_fails() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let sub = tokio::spawn({
        let client = client.clone();
        async move { client.subscribe_entity("sensor.temprature", |_, _| async { Ok(()) }).await }
    });
    let trigger = conn.recv_type("subscribe_trigger").await;
    conn.ok(&trigger["id"], Value::Null).await;
    let read = conn.recv_type("subscribe_entities").await;
    conn.ok(&read["id"], Value::Null).await;
    conn.event(&read["id"], json!({"a": {}})).await;

    let err = sub.await.unwrap().unwrap_err();
    assert!(matches!(&err, Error::Ha { code, .. } if code == "not_found"), "{err:?}");
    // Both server-side subscriptions are cleaned up.
    let mut unsubscribed = vec![
        conn.recv_type("unsubscribe_events").await["subscription"].clone(),
        conn.recv_type("unsubscribe_events").await["subscription"].clone(),
    ];
    unsubscribed.sort_by_key(|v| v.as_u64());
    assert_eq!(unsubscribed, [trigger["id"].clone(), read["id"].clone()]);
}

#[tokio::test]
async fn subscribing_while_disconnected_waits_for_the_connection() {
    let ha = MockHa::start().await;
    let config = ha.config().reconnect_delay(Duration::from_millis(300), Duration::from_millis(300));
    let (client, mut conn) = ha.connect(config).await;
    conn.ws.close(None).await.unwrap();
    wait_state(&client, |s| matches!(s, ConnectionState::Reconnecting { .. })).await;

    let (callback, mut rx) = collector();
    let sub = client.subscribe_trigger(TimePatternTrigger::new().seconds("/5"), callback).await;
    assert!(sub.is_ok(), "{sub:?}");
    assert_eq!(recv_within(&mut rx).await, "Interrupted");
    // Requests still fail fast.
    assert!(matches!(client.get_states().await, Err(Error::NotConnected)));

    let mut conn = ha.accept().await;
    let msg = conn.recv_type("subscribe_trigger").await;
    assert_eq!(msg["trigger"]["trigger"], "time_pattern");
    conn.ok(&msg["id"], Value::Null).await;
    assert_eq!(recv_within(&mut rx).await, "Restored");
    conn.event(&msg["id"], time_pattern_event(1)).await;
    assert_eq!(recv_within(&mut rx).await, "trigger 1");
}

#[tokio::test]
async fn stop_fails_in_flight_requests_with_stopped() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let req = tokio::spawn({
        let client = client.clone();
        async move { client.get_states().await }
    });
    conn.recv_type("get_states").await;
    client.stop().await;
    assert!(matches!(req.await.unwrap(), Err(Error::Stopped)));
}

/// `last_updated` of [`state`], as `subscribe_entities` sends it.
fn state_timestamp() -> f64 {
    let t = chrono::DateTime::parse_from_rfc3339("2026-10-28T14:45:09.185007+00:00").unwrap();
    t.timestamp_micros() as f64 / 1e6
}

#[tokio::test]
async fn abandoned_subscribe_is_cancelled() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let (callback, mut rx) = collector();
    let sub = {
        let client = client.clone();
        tokio::spawn(async move { client.subscribe_trigger(TimePatternTrigger::new().seconds("/5"), callback).await })
    };
    let msg = conn.recv_type("subscribe_trigger").await;
    // The caller gives up (e.g. its own timeout) before Home Assistant answers.
    sub.abort();
    assert!(sub.await.unwrap_err().is_cancelled());

    conn.ok(&msg["id"], Value::Null).await;
    let unsub = conn.recv_type("unsubscribe_events").await;
    assert_eq!(unsub["subscription"], msg["id"]);
    conn.ok(&unsub["id"], Value::Null).await;
    // The callback never runs and nothing is restored after a reconnect.
    conn.event(&msg["id"], time_pattern_event(1)).await;
    let mut conn = reconnect(&ha, &client, conn).await;
    round_trip(&client, &mut conn).await;
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn orphan_ids_are_forgotten_once_unsubscribed() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let req = tokio::spawn({
        let client = client.clone();
        async move { client.get_state("switch.pump").await }
    });
    let msg = conn.recv_type("subscribe_entities").await;
    answer_read(&mut conn, &msg, "switch.pump", "on", 100.0).await;
    req.await.unwrap().unwrap();
    let unsub = conn.recv_type("unsubscribe_events").await;
    // An event still in flight is ignored...
    conn.event(&msg["id"], json!({"c": {}})).await;
    conn.ok(&unsub["id"], Value::Null).await;
    round_trip(&client, &mut conn).await;
    // ...but once Home Assistant confirmed, the id is no longer tracked: an (impossible)
    // later event is treated as unknown and cancelled again.
    conn.event(&msg["id"], json!({"c": {}})).await;
    let again = conn.recv_type("unsubscribe_events").await;
    assert_eq!(again["subscription"], msg["id"]);
}

#[tokio::test]
async fn events_during_restore_wait_for_the_state_read() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let (mut rx, _) = subscribe_collecting(&client, &mut conn, "sensor.a").await;
    conn.ws.close(None).await.unwrap();
    assert_eq!(recv_within(&mut rx).await, "Interrupted");

    let mut conn = ha.accept().await;
    let sub = conn.recv_type("subscribe_trigger").await;
    conn.ok(&sub["id"], Value::Null).await;
    let read = conn.recv_type("subscribe_entities").await;
    // A live change arrives before the re-read: it changed 1 -> 2 while disconnected, then 2 -> 3.
    conn.event(&sub["id"], entity_trigger_event("sensor.a", "2", "3")).await;
    answer_read(&mut conn, &read, "sensor.a", "3", state_timestamp()).await;
    // One change spanning what was missed, then `Restored`; the held 2 -> 3 is covered by it.
    assert_eq!(recv_within(&mut rx).await, "1->3");
    assert_eq!(recv_within(&mut rx).await, "Restored");

    // The same update again (re-read and trigger raced) is not repeated.
    conn.event(&sub["id"], entity_trigger_event("sensor.a", "2", "3")).await;
    let mut later = entity_trigger_event("sensor.a", "3", "4");
    later["variables"]["trigger"]["to_state"]["last_updated"] = "2026-10-28T14:46:00+00:00".into();
    conn.event(&sub["id"], later).await;
    assert_eq!(recv_within(&mut rx).await, "3->4");
}

#[tokio::test]
async fn subscribe_in_flight_during_disconnect_is_kept() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let (callback, mut rx) = collector();
    let sub = {
        let client = client.clone();
        tokio::spawn(async move { client.subscribe_trigger(TimePatternTrigger::new().seconds("/5"), callback).await })
    };
    conn.recv_type("subscribe_trigger").await;
    conn.ws.close(None).await.unwrap();
    // Same as subscribing while disconnected.
    assert!(sub.await.unwrap().is_ok());
    assert_eq!(recv_within(&mut rx).await, "Interrupted");

    let mut conn = ha.accept().await;
    let msg = conn.recv_type("subscribe_trigger").await;
    conn.ok(&msg["id"], Value::Null).await;
    assert_eq!(recv_within(&mut rx).await, "Restored");
}

#[tokio::test]
async fn unsubscribe_is_reported_to_the_callback() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let (callback, mut rx) = collector();
    let sub = tokio::spawn({
        let client = client.clone();
        async move { client.subscribe_trigger(TimePatternTrigger::new().seconds("/5"), callback).await }
    });
    let msg = conn.recv_type("subscribe_trigger").await;
    conn.ok(&msg["id"], Value::Null).await;
    let sub = sub.await.unwrap().unwrap();

    let unsubscribe = tokio::spawn({
        let client = client.clone();
        async move { client.unsubscribe(sub).await }
    });
    let unsub = conn.recv_type("unsubscribe_events").await;
    assert_eq!(unsub["subscription"], msg["id"]);
    conn.ok(&unsub["id"], Value::Null).await;
    unsubscribe.await.unwrap().unwrap();
    assert_eq!(recv_within(&mut rx).await, "Unsubscribed");
}

#[tokio::test]
async fn dropping_a_guard_unsubscribes() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let sub = tokio::spawn({
        let client = client.clone();
        async move { client.subscribe_trigger(TimePatternTrigger::new().seconds("/5"), |_, _| async { Ok(()) }).await }
    });
    let msg = conn.recv_type("subscribe_trigger").await;
    conn.ok(&msg["id"], Value::Null).await;
    let guard = sub.await.unwrap().unwrap().guard(&client);
    drop(guard);
    let unsub = conn.recv_type("unsubscribe_events").await;
    assert_eq!(unsub["subscription"], msg["id"]);
}

#[tokio::test]
async fn stop_waits_for_queued_callbacks() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let sub = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .subscribe_trigger(TimePatternTrigger::new().seconds("/5"), move |_, event| {
                    let tx = tx.clone();
                    async move {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        tx.send(describe(&event))?;
                        Ok(())
                    }
                })
                .await
        }
    });
    let msg = conn.recv_type("subscribe_trigger").await;
    conn.ok(&msg["id"], Value::Null).await;
    sub.await.unwrap().unwrap();
    conn.event(&msg["id"], time_pattern_event(1)).await;
    round_trip(&client, &mut conn).await;

    tokio::time::timeout(WAIT, client.stop()).await.unwrap();
    assert_eq!(rx.try_recv().unwrap(), "trigger 1");
    assert_eq!(rx.try_recv().unwrap(), "Closed");
}

#[tokio::test]
async fn stop_from_a_callback_does_not_wait_for_itself() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let sub = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .subscribe_trigger(TimePatternTrigger::new().seconds("/5"), move |client, event| {
                    let tx = tx.clone();
                    async move {
                        if let Event::TriggerFired(_) = event {
                            client.stop().await;
                            tx.send(())?;
                        }
                        Ok(())
                    }
                })
                .await
        }
    });
    let msg = conn.recv_type("subscribe_trigger").await;
    conn.ok(&msg["id"], Value::Null).await;
    sub.await.unwrap().unwrap();
    conn.event(&msg["id"], time_pattern_event(1)).await;
    // Well within the 5s `stop()` would otherwise wait.
    tokio::time::timeout(Duration::from_secs(2), rx.recv()).await.unwrap().unwrap();
}

#[tokio::test]
async fn callbacks_accept_std_and_anyhow_errors() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let sub = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .subscribe_trigger(TimePatternTrigger::new().seconds("/5"), |client, _| async move {
                    use anyhow::Context;
                    client.turn_on("light.a").await?;
                    let n: u32 = "x".parse().context("parsing")?;
                    let _: u32 = "y".parse()?;
                    let _ = n;
                    Ok(())
                })
                .await
        }
    });
    let msg = conn.recv_type("subscribe_trigger").await;
    conn.ok(&msg["id"], Value::Null).await;
    sub.await.unwrap().unwrap();
}

#[tokio::test]
async fn state_only_entity_subscription_skips_attribute_updates() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;
    let (callback, mut rx) = collector();
    let sub = tokio::spawn({
        let client = client.clone();
        async move { client.subscribe_entity_with("sensor.a", EntityChanges::State, callback).await }
    });
    let msg = conn.recv_type("subscribe_trigger").await;
    // Home Assistant filters attribute-only changes itself...
    assert_eq!(msg["trigger"], json!({"trigger": "state", "entity_id": "sensor.a", "to": null}));
    conn.ok(&msg["id"], Value::Null).await;
    answer_state_read(&mut conn, "sensor.a", "1", 100.0).await;
    sub.await.unwrap().unwrap();

    // ...and the client does too, for whatever still keeps the state.
    conn.event(&msg["id"], entity_trigger_event("sensor.a", "1", "1")).await;
    conn.event(&msg["id"], entity_trigger_event("sensor.a", "1", "2")).await;
    assert_eq!(recv_within(&mut rx).await, "1->2");

    // Only an attribute changed while disconnected: no `StateChanged` before `Restored`.
    conn.ws.close(None).await.unwrap();
    assert_eq!(recv_within(&mut rx).await, "Interrupted");
    let mut conn = ha.accept().await;
    let sub = conn.recv_type("subscribe_trigger").await;
    conn.ok(&sub["id"], Value::Null).await;
    let read = conn.recv_type("subscribe_entities").await;
    answer_read(&mut conn, &read, "sensor.a", "2", 500.0).await;
    assert_eq!(recv_within(&mut rx).await, "Restored");
}
