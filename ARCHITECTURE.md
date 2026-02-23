# ha-core-client Architecture

This library is designed with type safety, asynchronicity, and resilience to network failures as core principles.

## Core Components

### 1. Handle/Runner Pattern (Actor Model)
The architecture decouples the user-facing interface from the network state management:
*   **`HAClient` (Handle)**: A lightweight structure containing only the communication channels (`mpsc::Sender`) to the background process. It implements `Clone`, allowing it to be easily passed into different async tasks or callbacks.
*   **`ClientRunner` (Runner/Actor)**: The central hub running in its own background task. It sequentially processes:
    *   Incoming commands from `HAClient`.
    *   Incoming WebSocket messages (events, API results).
    *   Shutdown signals.
    This design eliminates the need for complex locking mechanisms (`Mutex`/`RwLock`) for the client's shared state.

### 2. Connection Management (`WsConnection`)
A dedicated module manages the WebSocket lifecycle:
*   **Auto-Reconnection**: Upon connection loss, `WsConnection` attempts to reconnect with an exponential backoff strategy.
*   **Authentication State**: Handles the initial handshake and authentication using Long-Lived Access Tokens.
*   **Subscription Recovery**: On successful reconnection, the `ClientRunner` automatically resubscribes to all previously registered event handlers and triggers.

### 3. Type-Safe Protocol
*   **`protocol.rs`**: Defines strongly-typed structures for all Home Assistant WebSocket API messages. No raw JSON manipulation is used for core logic—everything is serialized and deserialized through these structures.
*   **`state/`**: Contains enums for domains, services, and entity states. Leveraging the `strum` crate, these enums are seamlessly converted to strings for the HA API.
*   **`events/`**: Implements intelligent deserialization for events, where attribute structures are automatically determined based on the entity's domain.

### 4. Subscription & Callback System
The library utilizes generalized async callbacks:
*   Users can provide any closure or function that returns a `Future`.
*   Callbacks receive their own clone of `HAClient`, enabling "reaction" logic (e.g., turning on a light in response to a motion sensor) without creating circular dependencies or deadlocks.

## Architectural Benefits
*   **Resilience**: The client automatically recovers even if the Home Assistant instance restarts.
*   **Ease of Use**: A high-level API hides the complexity of the underlying WebSocket protocol.
*   **Safety**: Rust's ownership model and the actor pattern guarantee the absence of memory errors and race conditions.
