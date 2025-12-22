use anyhow::{Context, Result};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use serde_json::json;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{error, info, warn};

async fn establish_ws_connection(
    url: &str,
    token: &str,
) -> Result<(
    SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
)> {
    info!("Connecting to {}", url);
    let (ws_stream, _) = connect_async(url)
        .await
        .context(format!("Failed to connect to {}", url))?;
    info!("WebSocket connected.");

    let (mut writer, mut reader) = ws_stream.split();

    // Отримання auth_required
    let auth_required = reader.next().await.context("No auth_required message")??;
    info!("Auth required: {:?}", auth_required);

    // TODO: Перевірити auth_required на успіх/невдачу

    // Відправка токену
    let auth_msg = json!({
        "type": "auth",
        "access_token": token
    })
    .to_string();
    writer.send(Message::Text(auth_msg.into())).await?;

    // Підтвердження аутентифікації
    let auth_result_msg = reader.next().await.context("No auth result")??;
    let auth_result_text = auth_result_msg.to_text()?;
    let auth_result_json: serde_json::Value = serde_json::from_str(auth_result_text)
        .context("Failed to parse auth result JSON")?;
    info!("Auth result: {}", auth_result_json);

    if auth_result_json.get("type").and_then(|v| v.as_str()) == Some("auth_invalid") {
        panic!("Authentication failed: Invalid token or credentials. Please check your configuration.");
    }

    if auth_result_json.get("type").and_then(|v| v.as_str()) != Some("auth_ok") {
        panic!("Authentication failed: Did not receive auth_ok. Got: {}", auth_result_json);
    }

    Ok((writer, reader))
}

pub struct WsConnection {
    // Канал для відправки повідомлень до WebSocket
    pub send_tx: mpsc::Sender<Message>,
    // Канал для отримання повідомлень з WebSocket
    pub recv_rx: mpsc::Receiver<Message>,
}

impl WsConnection {
    pub async fn new(url: String, token: String) -> Result<Self> {
        let (send_to_ws_tx, mut send_to_ws_rx) = mpsc::channel::<Message>(100); // HAClient sends here
        let (recv_from_ws_tx, recv_from_ws_rx) = mpsc::channel::<Message>(100); // HAClient receives from here

        tokio::spawn(async move {
            let retry_delay = std::time::Duration::from_secs(1);

            loop {
                match establish_ws_connection(&url, &token).await {
                    Ok((mut writer, mut reader)) => {
                        info!("Connection established and authenticated.");
                        let _ = recv_from_ws_tx
                            .send(Message::Text("CONNECTED".to_string().into()))
                            .await;

                        // Channels for this specific connection's reader/writer tasks
                        let (current_conn_send_tx, mut current_conn_send_rx) =
                            mpsc::channel::<Message>(100);

                        // Spawn reader task for this connection
                        let mut reader_task_handle = tokio::spawn({
                            let recv_from_ws_tx = recv_from_ws_tx.clone();
                            async move {
                                while let Some(msg) = reader.next().await {
                                    match msg {
                                        Ok(message) => {
                                            if let Err(e) = recv_from_ws_tx.send(message).await {
                                                error!(
                                                    "Failed to send received message to HAClient: {}",
                                                    e
                                                );
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            error!("WebSocket read error: {}", e);
                                            break;
                                        }
                                    }
                                }
                                error!("WebSocket reader task exited for current connection.");
                                let _ = recv_from_ws_tx
                                    .send(Message::Text("DISCONNECTED".to_string().into()))
                                    .await;
                            }
                        });

                        // Spawn writer task for this connection
                        let mut writer_task_handle = tokio::spawn(async move {
                            while let Some(message) = current_conn_send_rx.recv().await {
                                if let Err(e) = writer.send(message).await {
                                    error!("Failed to send message to WebSocket: {}", e);
                                    break;
                                }
                            }
                            error!("WebSocket writer task exited for current connection.");
                        });

                        // Forward messages from HAClient to current connection's writer
                        loop {
                            tokio::select! {
                                msg_to_send = send_to_ws_rx.recv() => {
                                    if let Some(msg) = msg_to_send {
                                        if current_conn_send_tx.send(msg).await.is_err() {
                                            error!("Failed to forward message to current connection writer, connection might be down.");
                                            break; // Break inner loop to try reconnect
                                        }
                                    } else {
                                        // send_to_ws_rx is closed, WsConnection was dropped
                                        info!("HAClient send channel closed, shutting down connection manager.");
                                        return;
                                    }
                                }
                                _ = &mut reader_task_handle => { // Check if reader task completed/failed
                                    error!("Reader task unexpectedly exited.");
                                    break;
                                }
                                _ = &mut writer_task_handle => { // Check if writer task completed/failed
                                    error!("Writer task unexpectedly exited.");
                                    break;
                                }
                            }
                        }
                        // Abort tasks if they are still running
                        reader_task_handle.abort();
                        writer_task_handle.abort();
                    }
                    Err(e) => {
                        error!("Failed to establish WebSocket connection: {}", e);
                    }
                }
                warn!("Attempting to reconnect in {:?}...", retry_delay);
                tokio::time::sleep(retry_delay).await;
            }
        });

        Ok(Self {
            send_tx: send_to_ws_tx,
            recv_rx: recv_from_ws_rx,
        })
    }
}
