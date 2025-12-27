use anyhow::{anyhow, Context, Result};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tracing::{error, info, warn};
use log::trace;

use super::protocol::{AuthMessage, BaseMessage, IncomingMsgType, OutgoingMsgType};

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

    // 1. Wait for auth_required
    let auth_required_msg = reader.next().await.context("Did not receive a message from the server")??;
    let auth_required_text = auth_required_msg.to_text()?;
    trace!("<<< Received Auth Required: {}", auth_required_text);
    let auth_required: BaseMessage = serde_json::from_str(auth_required_text)
        .context("Failed to parse the first message from the server")?;

    if auth_required.msg_type != IncomingMsgType::AuthRequired {
        return Err(anyhow!(
            "Expected 'auth_required', but got '{:?}'",
            auth_required.msg_type
        ));
    }
    info!("'auth_required' message received.");

    // 2. Send auth token
    let auth_msg = AuthMessage {
        msg_type: OutgoingMsgType::Auth,
        access_token: token,
    };
    let auth_str = serde_json::to_string(&auth_msg).context("Failed to serialize auth message")?;
    trace!(">>> Sending Auth: {}", auth_str);
    writer.send(Message::Text(auth_str.into())).await?;
    info!("Authentication message sent.");

    // 3. Wait for auth result
    let auth_result_msg = reader.next().await.context("Did not receive auth result")??;
    let auth_result_text = auth_result_msg.to_text()?;
    trace!("<<< Received Auth Result: {}", auth_result_text);
    let auth_result: BaseMessage = serde_json::from_str(auth_result_text)
        .context("Failed to parse auth result")?;

    match auth_result.msg_type {
        IncomingMsgType::AuthOk => {
            info!("Authentication successful ('auth_ok').");
            Ok((writer, reader))
        }
        IncomingMsgType::AuthInvalid => Err(anyhow!(
            "Authentication failed: Invalid token or credentials ('auth_invalid'). Please check your configuration."
        )),
        other => Err(anyhow!(
            "Authentication failed: Expected 'auth_ok' or 'auth_invalid', but got '{:?}'",
            other
        )),
    }
}

pub struct WsConnection {
    // Канал для відправки повідомлень до WebSocket
    pub send_tx: mpsc::Sender<Message>,
    // Канал для отримання повідомлень з WebSocket
    pub recv_rx: mpsc::Receiver<Message>,
}

impl WsConnection {
    pub async fn new(url: &str, token: &str) -> Result<Self> {
        let (send_to_ws_tx, mut send_to_ws_rx) = mpsc::channel::<Message>(100); // HAClient sends here
        let (recv_from_ws_tx, recv_from_ws_rx) = mpsc::channel::<Message>(100); // HAClient receives from here

        let url = url.to_string();
        let token = token.to_string();

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
                                            if let Message::Text(text) = &message {
                                                trace!("<<< Received: {}", text);
                                            }
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
                                if let Message::Text(text) = &message {
                                    trace!(">>> Sending: {}", text);
                                }
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
