use futures_util::{SinkExt, StreamExt};
use log::debug;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async_tls_with_config, MaybeTlsStream, WebSocketStream};

use crate::config::ClientConfig;
use crate::error::{Error, Result};
use crate::session::Session;

pub(crate) type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Opens the WebSocket (no authentication yet), within `connect_timeout`.
pub(crate) async fn open(config: &ClientConfig) -> Result<WsStream> {
    tokio::time::timeout(config.connect_timeout, open_ws(config))
        .await
        .map_err(|_| Error::Connection(format!("timed out connecting to {}", config.url)))?
}

async fn open_ws(config: &ClientConfig) -> Result<WsStream> {
    debug!("Connecting to {}", config.url);
    // Answers arrive as one frame each, so frames may be as large as messages.
    let limit = Some(config.max_message_size);
    let ws_config = WebSocketConfig::default().max_message_size(limit).max_frame_size(limit);
    let (ws, _) = connect_async_tls_with_config(config.url.as_str(), Some(ws_config), false, config.tls_connector.clone())
        .await
        .map_err(|e| Error::Connection(format!("failed to connect to {}: {e}", config.url)))?;
    Ok(ws)
}

/// Opens the WebSocket and completes the authentication with `session`, for the first
/// connect of [`HAClient::connect`](crate::HAClient::connect), which reports a failure to
/// the caller instead of retrying. `session` must have handed out `Action::Connect`.
pub(crate) async fn connect_and_auth(session: &mut Session) -> Result<WsStream> {
    let config = session.config().clone();
    tokio::time::timeout(config.connect_timeout, async {
        let mut ws = open_ws(&config).await?;
        session.on_connected(now());
        loop {
            while let Some(frame) = session.poll_transmit() {
                ws.send(Message::Text(frame.into()))
                    .await
                    .map_err(|e| Error::Connection(format!("failed to send auth message: {e}")))?;
            }
            if session.is_connected() {
                return Ok(ws);
            }
            match ws.next().await {
                Some(Ok(Message::Text(text))) => session.on_frame(now(), &text),
                Some(Ok(Message::Close(frame))) => {
                    return Err(Error::Connection(format!("server closed the connection during handshake: {frame:?}")));
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(Error::Connection(format!("handshake read error: {e}"))),
                None => return Err(Error::Connection("connection closed during handshake".into())),
            }
            if let Some(error) = session.take_connect_error() {
                return Err(error);
            }
        }
    })
    .await
    .unwrap_or_else(|_| Err(Error::Connection(format!("timed out connecting to {}", config.url))))
}

fn now() -> std::time::Instant {
    tokio::time::Instant::now().into_std()
}

/// Rejects URLs that are not WebSocket URLs, with a hint for the common `http://` mistake.
pub(crate) fn check_url(url: &str) -> Result<()> {
    let scheme = url.split_once("://").map(|(scheme, _)| scheme.to_ascii_lowercase());
    match scheme.as_deref() {
        Some("ws" | "wss") => Ok(()),
        Some(http @ ("http" | "https")) => Err(Error::InvalidInput(format!(
            "{url} is not a WebSocket URL; use {}://<host>:8123/api/websocket",
            if http == "https" { "wss" } else { "ws" }
        ))),
        _ => Err(Error::InvalidInput(format!("{url} is not a ws:// or wss:// URL"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_check() {
        assert!(check_url("ws://localhost:8123/api/websocket").is_ok());
        assert!(check_url("WSS://ha.example/api/websocket").is_ok());
        let err = check_url("http://localhost:8123").unwrap_err().to_string();
        assert!(err.contains("use ws://"), "{err}");
        assert!(check_url("https://ha.example").unwrap_err().to_string().contains("use wss://"));
        assert!(matches!(check_url("localhost:8123"), Err(Error::InvalidInput(_))));
    }
}
