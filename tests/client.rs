//! End-to-end tests against a scripted mock of the Home Assistant WebSocket API.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use ha_core_client::events::Event;
use ha_core_client::state::{Domain, Service};
use ha_core_client::{ClientConfig, ConnectionState, Error, HAClient};
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

/// Subscribes to `entity_id`, acknowledges on the mock side, and forwards new states to a channel.
async fn subscribe_collecting(client: &HAClient, conn: &mut Conn, entity_id: &str) -> (mpsc::UnboundedReceiver<String>, Value) {
    let (tx, rx) = mpsc::unbounded_channel();
    let sub = {
        let client = client.clone();
        let entity_id = entity_id.to_string();
        tokio::spawn(async move {
            client
                .subscribe_entity(&entity_id, move |_, event| {
                    let tx = tx.clone();
                    async move {
                        if let Event::StateChanged { new_state: Some(s), .. } = event {
                            tx.send(s.state).unwrap();
                        }
                        Ok(())
                    }
                })
                .await
        })
    };
    let msg = conn.recv_type("subscribe_trigger").await;
    assert_eq!(msg["trigger"], json!({"platform": "state", "entity_id": entity_id}));
    conn.ok(&msg["id"], Value::Null).await;
    sub.await.unwrap().unwrap();
    (rx, msg["id"].clone())
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
async fn entity_events_are_delivered_in_order() {
    let ha = MockHa::start().await;
    let (client, mut conn) = ha.connect(ha.config()).await;

    // The callback sleeps longer for earlier events; order must still be preserved.
    let (tx, mut rx) = mpsc::unbounded_channel();
    let sub = {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .subscribe_entity("sensor.power", move |_, event| {
                    let tx = tx.clone();
                    async move {
                        let Event::StateChanged { new_state: Some(s), old_state: Some(_), .. } = event else {
                            anyhow::bail!("unexpected event {event:?}");
                        };
                        let n: u64 = s.state.parse()?;
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
        conn.event(&msg["id"], entity_trigger_event("sensor.power", "0", &n.to_string())).await;
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
            client.subscribe_template_trigger("{{ invalid", None, |_, _| async { Ok(()) }).await
        })
    };
    let msg = conn.recv_type("subscribe_trigger").await;
    assert_eq!(msg["trigger"]["platform"], "template");
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

    conn.event(&second["id"], entity_trigger_event("sensor.b", "1", "2")).await;
    conn.event(&first["id"], entity_trigger_event("sensor.a", "1", "3")).await;
    assert_eq!(recv_within(&mut rx_b).await, "2");
    assert_eq!(recv_within(&mut rx_a).await, "3");

    // Requests work again on the new connection.
    let req = tokio::spawn({
        let client = client.clone();
        async move { client.get_states().await }
    });
    let msg = conn.recv_type("get_states").await;
    conn.ok(&msg["id"], json!([state("sensor.a", "3")])).await;
    assert_eq!(req.await.unwrap().unwrap().len(), 1);
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
