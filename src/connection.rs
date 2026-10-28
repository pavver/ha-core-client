use futures_util::{SinkExt, StreamExt};
use log::{debug, info, warn};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async_with_config, MaybeTlsStream, WebSocketStream};

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
    // Answers arrive as one frame each, so frames may be as large as messages.
    let limit = Some(config.max_message_size);
    let ws_config = WebSocketConfig::default().max_message_size(limit).max_frame_size(limit);
    let (mut ws, _) = connect_async_with_config(config.url.as_str(), Some(ws_config), false)
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
            if let Some(version) = reply.ha_version.as_deref().filter(|v| !is_supported(v)) {
                warn!(
                    "Home Assistant {version} is older than {}.{}; trigger subscriptions will be rejected.",
                    MIN_HA_VERSION.0, MIN_HA_VERSION.1
                );
            }
            Ok(ws)
        }
        "auth_invalid" => Err(Error::Auth(reply.message.unwrap_or_else(|| "invalid access token".into()))),
        other => Err(Error::Protocol(format!("expected 'auth_ok' or 'auth_invalid', got '{other}'"))),
    }
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

    #[test]
    fn version_check() {
        assert!(is_supported("2026.9.4"));
        assert!(is_supported("2024.10.0"));
        assert!(!is_supported("2024.9.3"));
        assert!(!is_supported("2023.12.1"));
        assert!(is_supported("dev"));
    }
}
