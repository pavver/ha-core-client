use futures_util::{SinkExt, StreamExt};
use log::{debug, info};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use crate::config::ClientConfig;
use crate::error::{Error, Result};
use crate::protocol::{self, Incoming};

pub(crate) type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Opens the WebSocket and completes the `auth_required` → `auth` → `auth_ok` handshake.
pub(crate) async fn connect_and_auth(config: &ClientConfig) -> Result<WsStream> {
    tokio::time::timeout(config.connect_timeout, handshake(config))
        .await
        .map_err(|_| Error::Connection(format!("timed out connecting to {}", config.url)))?
}

async fn handshake(config: &ClientConfig) -> Result<WsStream> {
    debug!("Connecting to {}", config.url);
    let (mut ws, _) = connect_async(config.url.as_str())
        .await
        .map_err(|e| Error::Connection(format!("failed to connect to {}: {e}", config.url)))?;

    let first = next_json(&mut ws).await?;
    if first.msg_type != "auth_required" {
        return Err(Error::Protocol(format!("expected 'auth_required', got '{}'", first.msg_type)));
    }

    // The token is never logged.
    ws.send(Message::Text(protocol::auth(&config.token).into()))
        .await
        .map_err(|e| Error::Connection(format!("failed to send auth message: {e}")))?;

    let reply = next_json(&mut ws).await?;
    match reply.msg_type.as_str() {
        "auth_ok" => {
            info!("Connected and authenticated to {}", config.url);
            Ok(ws)
        }
        "auth_invalid" => Err(Error::Auth(reply.message.unwrap_or_else(|| "invalid access token".into()))),
        other => Err(Error::Protocol(format!("expected 'auth_ok' or 'auth_invalid', got '{other}'"))),
    }
}

async fn next_json(ws: &mut WsStream) -> Result<Incoming> {
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(text))) => {
                return serde_json::from_str(&text)
                    .map_err(|e| Error::Protocol(format!("invalid handshake message: {e}")));
            }
            Some(Ok(Message::Close(frame))) => {
                return Err(Error::Connection(format!("server closed the connection during handshake: {frame:?}")));
            }
            Some(Ok(_)) => continue,
            Some(Err(e)) => return Err(Error::Connection(format!("handshake read error: {e}"))),
            None => return Err(Error::Connection("connection closed during handshake".into())),
        }
    }
}
