use std::fmt;
use std::time::Duration;

/// Upper bound for every configured duration, so arithmetic on deadlines cannot overflow.
/// Larger values (e.g. `Duration::MAX` meaning "never") are clamped to this.
const MAX_DURATION: Duration = Duration::from_secs(365 * 24 * 3600);

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
    pub(crate) subscription_queue_size: usize,
    pub(crate) slow_callback_warning: Duration,
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
            subscription_queue_size: 1024,
            slow_callback_warning: Duration::from_secs(5),
        }
    }

    /// Timeout for establishing the connection and completing authentication. Default: 10s.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout.min(MAX_DURATION);
        self
    }

    /// How long a request waits for Home Assistant's answer before failing with
    /// [`Error::Timeout`](crate::Error::Timeout). Default: 30s.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout.min(MAX_DURATION);
        self
    }

    /// Idle time after which the client sends a `ping` to check the connection. Default: 30s.
    pub fn ping_interval(mut self, interval: Duration) -> Self {
        self.ping_interval = interval.min(MAX_DURATION);
        self
    }

    /// How long to wait for a `pong`, or for a single write to complete, before treating
    /// the connection as dead. Default: 10s.
    pub fn pong_timeout(mut self, timeout: Duration) -> Self {
        self.pong_timeout = timeout.min(MAX_DURATION);
        self
    }

    /// Bounds of the exponential reconnect backoff. Defaults: 1s and 60s.
    pub fn reconnect_delay(mut self, min: Duration, max: Duration) -> Self {
        self.reconnect_min_delay = min.min(MAX_DURATION);
        self.reconnect_max_delay = max.clamp(self.reconnect_min_delay, MAX_DURATION);
        self
    }

    /// Maximum number of events waiting for one subscription's callback. Default: 1024.
    ///
    /// Callbacks of a subscription run one at a time. When one falls behind, entity
    /// subscriptions merge consecutive state changes (so they never queue more than one),
    /// and other subscriptions drop their oldest events once this limit is reached. Either
    /// way the callback then receives `SubscriptionStatus::Lagged`. Status events are never
    /// dropped and don't count towards the limit.
    pub fn subscription_queue_size(mut self, size: usize) -> Self {
        self.subscription_queue_size = size.max(1);
        self
    }

    /// Logs a warning when a single callback runs longer than this. `Duration::ZERO`
    /// disables the warning. Default: 5s.
    pub fn slow_callback_warning(mut self, threshold: Duration) -> Self {
        self.slow_callback_warning = threshold.min(MAX_DURATION);
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
            .field("subscription_queue_size", &self.subscription_queue_size)
            .field("slow_callback_warning", &self.slow_callback_warning)
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
