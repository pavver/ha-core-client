# ha-core-client Architecture

This library is designed with type safety, asynchronicity, and resilience to network failures as core principles.

## Core Components

### 1. Handle/Runner Pattern (Actor Model)
*   **`HAClient` (Handle, `src/lib.rs`)**: holds an `mpsc::Sender<Command>` and an `Arc<Shared>` (config, shutdown signal, connection-state `watch`). Cloning is cheap. Every method sends a `Command` (`src/subscription.rs`) carrying a `oneshot` responder and awaits it with the request timeout.
*   **`Runner` (Actor, `src/runner.rs`)**: a single background task that owns the WebSocket and all state (subscriptions, in-flight requests, per-connection message ids). No locks are needed.
*   The runner keeps only a *weak* sender, so it shuts down when every user handle is dropped. Callbacks get a temporary strong handle while they run.

### 2. Connection Lifecycle
*   `HAClient::connect` performs the first connect and authentication itself (`src/connection.rs`), so a wrong URL or token is returned to the caller instead of being retried.
*   A **session** runs while connected: `select!` over shutdown, incoming frames, commands and a housekeeping tick.
*   A session ends on a `Close` frame, read/write error, or a missing `pong`. All in-flight requests then fail with `ConnectionLost`, and the runner enters the **offline** phase: exponential backoff with jitter, then reconnect. The backoff starts over only after a session that lasted at least `min(reconnect_max_delay, 60s)`, so a server that drops the connection right after `auth_ok` is not hammered. While offline, commands are answered immediately with `NotConnected`.
*   `auth_invalid` during a reconnect is terminal (`ConnectionState::AuthFailed`), because retrying a revoked token only risks an IP ban.

### 3. Message Ids and Subscriptions
*   Triggers are sent with the `trigger` key, so Home Assistant 2024.10+ is required; `connect` logs a warning for an older `ha_version`.
*   Home Assistant requires message ids to strictly increase per connection, so ids restart at 1 on each connection.
*   Each subscription has a stable client-side id (the `Subscription` handle) plus the Home Assistant message id on the current connection. A `routes` map translates the latter back to the former.
*   On reconnect, all subscriptions are re-sent in creation order (a `BTreeMap`). A re-subscription that Home Assistant rejects is logged and dropped.
*   `subscribe_*` calls complete when Home Assistant acknowledges them, so configuration errors reach the caller. Handing the `Subscription` back goes through `Runner::confirm`: if the caller stopped waiting, the subscription is removed (queue discarded) and unsubscribed, since nobody could ever unsubscribe it.
*   Subscriptions learn about the connection through `Event::Status`, which goes through the same per-subscription queue as data events: `Interrupted` when a session is lost, `Restored` once re-acknowledged, `Rejected` if Home Assistant refuses the re-subscription (it is then dropped), `Unsubscribed` after `unsubscribe`, `Closed` when the client stops.
*   A `Subscribe` received while offline, or still unanswered when the connection drops, is registered locally (marked interrupted, `Interrupted` delivered) and sent by the next `resubscribe`.
*   For `subscribe_entity`, the caller's responder is carried by the initial `Resync` read, so a missing entity fails the call (both server-side subscriptions are then unsubscribed).
*   Entity subscriptions keep their last seen state. After subscribing, and again after each re-subscription, the runner reads the entity with a one-shot `subscribe_entities` (`PendingKind::Resync`). After a reconnect, live trigger events are held (`SubEntry::held`) until that read arrives; then a difference from the last seen state is delivered as one `StateChanged`, then `Restored`, then the held events the read does not already cover (`covered` compares `last_updated`). Outside a restore, an event for the exact update already in `last_state` is skipped (the read and the trigger can both report it). A re-subscription that times out is sent again on the same connection (neither dropped nor forcing a reconnect, which would interrupt every other subscription); if Home Assistant answers the old request late, the duplicate is cancelled on its first event as an unknown id.
*   Home Assistant may send a subscription's first event before its `result` (`render_template` does). Events for an id still in `PendingKind::Subscribe` go to `SubEntry::early` and are queued after the ack (after `Restored` on a reconnect); for entity subscriptions they become held events (restore) or are covered by the baseline read.

### 4. Request Bookkeeping
*   Every sent request has a `Pending` entry with a deadline. The housekeeping tick expires overdue entries (`Timeout`) and drives the `ping`/`pong` keep-alive.
*   Each write is bounded by `pong_timeout`; a blocked write (half-open connection) ends the session.
*   Server-side subscriptions the client no longer routes (one-shot reads, unsubscribed, timed out) are unsubscribed once (`PendingKind::Unsubscribe`); their ids sit in `orphans` so late events are ignored, and are removed when Home Assistant confirms (nothing arrives after that answer).
*   The session `select!` is not `biased`, so a steady stream of frames cannot starve commands or the housekeeping tick.
*   A `Drop` guard on the runner sets `ConnectionState::Stopped` if the task ever ends without `finish` (a panic), so waiters are released.
*   `get_state` uses a one-shot `subscribe_entities`: the ack is followed by an event with the compressed state, after which the client unsubscribes.

### 5. Type-Safe Protocol
*   **`protocol.rs`**: builders for outgoing messages and one `Incoming` struct for everything Home Assistant sends.
*   **`state/`**: enums for domains, services, entity states and attribute values. String-valued enums are generated by `string_enum!` with an `Other(String)` fallback, so unknown values round-trip instead of failing.
*   **`events/`**: `Event` conversion per subscription kind: bus events, entity state triggers (as `StateChanged`), other triggers (as `TriggerFired`), `render_template` results (as `TemplateRendered`/`TemplateError`, or `Silence` for `watch_silence`). Attributes are typed by the entity's domain (`AttributeSet::for_entity`, also for history entries) and parsing never fails; values that do not fit the typed schema are kept untyped.

### 6. Callbacks
*   Each subscription has a worker task that runs its callbacks one at a time, in event order. Each callback runs in its own spawned task, so a panic is contained and logged. A callback running longer than `slow_callback_warning` is logged once when it crosses the threshold and again when it finishes.
*   Runner and worker are connected by a bounded queue (`src/queue.rs`). The runner never waits on it. When a callback falls behind, entity subscriptions merge the newest `StateChanged` into the queued one; other subscriptions drop their oldest data event past `subscription_queue_size`. Status events bypass the limit. For `EntityChanges::State` subscriptions (`queue::Filter::StateChanges`) the queue also drops changes that keep the state string, including a merge that cancels out (`A → B → A`), which also covers the post-reconnect `StateChanged`; the server-side trigger gets `to: null` so attribute-only changes are not even sent. The count of merged/dropped events is handed to the callback as `SubscriptionStatus::Lagged` before the next event.
*   The worker holds only a weak command sender, so queued events don't keep a dropped client alive.
*   `Shared::workers` counts live workers; `stop()` waits for it to reach zero (bounded by 5s). Callbacks run inside the `IN_CALLBACK` task-local, so `stop()` called from a callback skips that wait instead of waiting for itself.
*   Callbacks return `CallbackResult` (`Result<(), BoxError>`), so the public API does not depend on `anyhow`.
*   Callbacks receive an `HAClient` handle, so they can call Home Assistant themselves without deadlocking the runner.
