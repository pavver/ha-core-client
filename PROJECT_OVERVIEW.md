# Project Overview

This project is a client for the Home Assistant WebSocket API, written in Rust. It's designed to be a modular and resilient client that can handle WebSocket connections, authentication, and subscriptions. Its key features include:

*   **Type-Safe Events**: Automatically deserializes incoming events into a strongly-typed `enum`, making handlers robust and easy to write.
*   **Async Callbacks**: Event handlers are native `async` functions, removing boilerplate like `tokio::spawn`.
*   **Concurrent Operations**: Handlers can make their own API calls back to Home Assistant (e.g., get the state of another sensor or call a service) without causing deadlocks.
*   **Idiomatic Error Handling**: Callbacks return `Result` allowing for the use of the `?` operator for concise and consistent error propagation.
*   **Auto-Reconnection**: Automatically handles WebSocket connection and re-subscription logic.

## Project Structure

The project has been refactored into a more modular and maintainable structure. The core `client` logic is now split across several focused files.

```
.
├── src
│   ├── client
│   │   ├── mod.rs              # Core HAClient struct, main loop, and module declarations.
│   │   ├── connection.rs       # Handles the raw WebSocket connection and auto-reconnect.
│   │   ├── events.rs           # Defines strongly-typed structs and enums for HA events.
│   │   ├── subscription.rs     # Defines the core EventHandlerContext and subscription types.
│   │   ├── context_helpers.rs  # Implements all ergonomic helper methods for EventHandlerContext.
│   │   ├── state.rs            # Defines strongly-typed enums for HA states and service parameters.
│   │   ├── handler.rs          # Contains the message handler logic for the HAClient.
│   │   └── pending.rs          # Defines structs for managing pending API calls.
│   ├── config.rs               # Configuration reading logic.
│   └── main.rs                 # The main application entry point and usage example.
├── Cargo.toml
├── config.json
└── ...
```

*   **`main.rs`**: The application's entry point. Initializes the logger, reads configuration, creates an `HAClient`, subscribes to an entity, and starts the client's event loop.
*   **`config.rs`**: Contains the logic to read and parse the `config.json` file.
*   **`client/` module**: The core of the library, containing the `HAClient` implementation.
    *   **`client/mod.rs`**: The central hub of the `client` module. It defines the main `HAClient` struct, orchestrates the primary `run` loop, and declares the other modules.
    *   **`client/connection.rs`**: Implements the transport layer. The `WsConnection` struct manages the WebSocket connection lifecycle.
    *   **`client/events.rs`**: Defines the core `events::Event` enum and associated data structures (e.g., `State`). This provides a type-safe representation of incoming Home Assistant events, replacing raw `serde_json::Value`.
    *   **`client/subscription.rs`**: Defines the public API data structures, including `EventHandlerContext`, the internal `HandlerCommand`, and the `EventCallback` type.
    *   **`client/context_helpers.rs`**: Implements user-friendly helper methods for `EventHandlerContext` (e.g., `set_number`, `turn_on`).
    *   **`client/state.rs`**: Provides strongly-typed Rust enums for Home Assistant states (`HaState`) and service parameters (e.g., `HvacMode`).
    *   **`client/handler.rs`**: Contains the logic for processing incoming WebSocket messages, including deserializing event data into the `events::Event` enum.
    *   **`client/pending.rs`**: Defines structs used to track in-flight API calls.

## How it Works

The client's architecture is designed for concurrency, type-safety, and ease of use.

1.  **Asynchronous by Default**: The entire client is built on `tokio`, Rust's asynchronous runtime. All I/O and event handling operations are non-blocking.

2.  **Central Event Loop**: The `HAClient::run` method is the master event loop. It uses a `tokio::select!` macro to simultaneously process messages from two sources:
    *   **WebSocket Messages**: Incoming data from Home Assistant (events, API call results, etc.).
    *   **Handler Commands**: Internal commands sent from user-defined event callbacks (e.g., "get state", "call service").

3.  **Type-Safe, Concurrent Event Handling with Idiomatic Error Propagation**:
    *   **Deserialization**: When an event arrives from Home Assistant, `handler.rs` checks the subscription's `event_type`. It then attempts to deserialize the JSON payload into a matching, strongly-typed variant of the `client::events::Event` enum (e.g., `Event::StateChanged`). If the event type is not specifically handled, it gracefully falls back to an `Event::Generic` variant containing the raw data.
    *   **Async Callbacks**: The `EventCallback` is an `async` function that receives the `EventHandlerContext` and the typed `events::Event` enum. It returns `Result<()>`, allowing you to use `match` or `if let` for robust, compile-time checked event processing, and the `?` operator for concise error propagation.
    *   **Error Handling**: If a user-defined callback returns an `Err`, the `HAClient`'s main event loop catches this error and logs it, ensuring that individual callback failures do not crash the entire client.
    *   **Concurrent Execution**: The `async` callback returns a `Future`. The `HAClient`'s main loop immediately `tokio::spawn`s this future. This runs the handler's logic in a new, separate task, allowing multiple handlers to execute in parallel and preventing any single handler from blocking the main client.

4.  **The `EventHandlerContext`**:
    *   This context object is the key to enabling powerful, stateful handlers. It is given to every callback and exposes `async` helper methods like `get_state(...)`, `turn_on(...)`, and `set_temperature(...)`. These methods also return `Result` types, allowing their usage with the `?` operator within your callbacks.
    *   When a handler calls one of these methods, the context creates a `HandlerCommand` and sends it over an `mpsc` channel to the main `HAClient::run` loop.
    *   The method then `await`s a response on a temporary `oneshot` channel, without blocking the main client loop.

5.  **Request-Response Flow**:
    *   The `HAClient::run` loop receives the `HandlerCommand`.
    *   It generates a unique ID for the API call and sends the appropriate message (e.g., `get_states` or `call_service`) to Home Assistant over the WebSocket.
    *   It stores the `responder` part of the `oneshot` channel in a `pending_api_calls` map, keyed by the unique ID.
    *   When Home Assistant sends back a `result` message with that ID, the `run` loop looks up the pending call, extracts the result, and sends it back via the `responder`.
    *   The `await` in the event handler's context method completes, and the handler continues its logic with the result of the API call.

This design makes the client highly resilient and ergonomic, enabling complex, chained automations where one event can trigger a series of queries and actions.
