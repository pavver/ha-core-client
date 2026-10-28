use std::fmt;
use std::time::Duration;

/// Connection settings for [`HAClient::connect_with`](crate::HAClient::connect_with).
///
/// ```
/// use std::time::Duration;
/// use ha_core_client::ClientConfig;
///
/// let config = ClientConfig::new("ws://homeassistant.local:8123/api/websocket", "TOKEN")
///     .request_timeout(Duration::from_secs(10));
/// ```
#[derive(Clone)]
pub struct ClientConfig {
    pub(crate) url: String,
    pub(crate) token: String,
    pub(crate) connect_timeout: Duration,
    pub(crate) request_timeout: Duration,
    pub(crate) ping_interval: Duration,
    pub(crate) pong_timeout: Duration,
    pub(crate) reconnect_min_delay: Duration,
    pub(crate) reconnect_max_delay: Duration,
}

impl ClientConfig {
    /// Creates a config with default timeouts.
    ///
    /// * `url` - WebSocket URL, e.g. `ws://localhost:8123/api/websocket`.
    /// * `token` - A Long-Lived Access Token generated in Home Assistant.
    pub fn new(url: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            token: token.into(),
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            ping_interval: Duration::from_secs(30),
            pong_timeout: Duration::from_secs(10),
            reconnect_min_delay: Duration::from_secs(1),
            reconnect_max_delay: Duration::from_secs(60),
        }
    }

    /// Timeout for establishing the connection and completing authentication. Default: 10s.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// How long a request waits for Home Assistant's answer before failing with
    /// [`Error::Timeout`](crate::Error::Timeout). Default: 30s.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Idle time after which the client sends a `ping` to check the connection. Default: 30s.
    pub fn ping_interval(mut self, interval: Duration) -> Self {
        self.ping_interval = interval;
        self
    }

    /// How long to wait for a `pong` before treating the connection as dead. Default: 10s.
    pub fn pong_timeout(mut self, timeout: Duration) -> Self {
        self.pong_timeout = timeout;
        self
    }

    /// Bounds of the exponential reconnect backoff. Defaults: 1s and 60s.
    pub fn reconnect_delay(mut self, min: Duration, max: Duration) -> Self {
        self.reconnect_min_delay = min;
        self.reconnect_max_delay = max.max(min);
        self
    }
}

impl fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientConfig")
            .field("url", &self.url)
            .field("token", &"<redacted>")
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("ping_interval", &self.ping_interval)
            .field("pong_timeout", &self.pong_timeout)
            .field("reconnect_min_delay", &self.reconnect_min_delay)
            .field("reconnect_max_delay", &self.reconnect_max_delay)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_token() {
        let cfg = ClientConfig::new("ws://x", "secret-token");
        let s = format!("{cfg:?}");
        assert!(!s.contains("secret-token"));
        assert!(s.contains("<redacted>"));
    }
}
