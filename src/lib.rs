//! A strongly-typed, resilient asynchronous client for the Home Assistant WebSocket API.
//!
//! ```no_run
//! use ha_core_client::{HAClient, Update};
//!
//! # async fn example() -> ha_core_client::Result<()> {
//! let client = HAClient::connect("ws://homeassistant.local:8123/api/websocket", "TOKEN").await?;
//!
//! client.subscribe_entity("binary_sensor.motion", |client, update| async move {
//!     if let Update::Event(change) = update {
//!         if change.new_state.is_some_and(|state| state.state == "on") {
//!             client.turn_on("light.hallway").await?;
//!         }
//!     }
//!     Ok(())
//! }).await?;
//!
//! // Runs until `stop()` is called or authentication fails permanently.
//! client.closed().await;
//! # Ok(())
//! # }
//! ```

// Without `runtime` the Tokio client is gone, and with it the users of a few crate-private helpers.
#![cfg_attr(not(feature = "runtime"), allow(dead_code))]

#[cfg(feature = "runtime")]
mod client;
mod config;
#[cfg(feature = "runtime")]
mod connection;
#[cfg(feature = "runtime")]
mod context_helpers;
mod error;
pub mod events;
mod protocol;
mod queue;
#[cfg(feature = "runtime")]
mod runner;
pub mod session;
mod source;
pub mod state;
#[cfg(feature = "runtime")]
mod stream;
mod subscription;
mod target;
pub mod triggers;

#[cfg(feature = "runtime")]
pub use client::{HAClient, WeakClient};
pub use config::ClientConfig;
#[cfg(feature = "runtime")]
pub use tokio_tungstenite::Connector as TlsConnector;
pub use error::{Error, HaErrorCode, HaErrorTranslation, Result};
pub use target::Target;
pub use events::{BusEvent, Context, ServiceCall, Silence, State, StateChange, SubscriptionStatus, TemplateUpdate, TriggerFired, Update};
pub use session::{Action, ConnectionState, PendingSubscription, Reply, Session, SessionEvents};
pub use source::Source;
#[cfg(feature = "runtime")]
pub use stream::EventStream;
#[cfg(feature = "runtime")]
pub use subscription::SubscriptionGuard;
pub use subscription::{BoxError, CallbackResult, EntityChanges, Subscription};
