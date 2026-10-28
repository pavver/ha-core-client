# Project Overview

`ha-core-client` is a library crate: a strongly-typed, resilient client for the Home Assistant WebSocket API.

*   Usage and API: [README.md](README.md) and [HA_CORE_CLIENT_SKILL.md](HA_CORE_CLIENT_SKILL.md).
*   Internals: [ARCHITECTURE.md](ARCHITECTURE.md).
*   Changes and migration: [CHANGELOG.md](CHANGELOG.md).
*   Trigger platforms reference: [TRIGGERS_OVERVIEW.md](TRIGGERS_OVERVIEW.md).

## Future Work / Planned Features

Below is a list of Home Assistant WebSocket API features that are not yet implemented in this client.

**Note: Do not begin implementation of these features without a specific command to do so.**

### Core Commands
*   **~~`unsubscribe_events`~~**: (Implemented) Відписується від подій, на які раніше була оформлена підписка. Це дія, протилежна до `subscribe_events`.
*   **`fire_event`**: Дозволяє ініціювати (викликати) власну подію в системній шині подій Home Assistant.
*   **~~`subscribe_trigger`~~**: (Implemented) Підписується на спрацювання одного або кількох тригерів, аналогічно до того, як це працює в автоматизаціях Home Assistant.

### Configuration & Information Commands
*   **`get_config`**: Запитує та отримує поточну конфігурацію екземпляра Home Assistant.
*   **`get_services`**: Отримує повний і детальний список усіх сервісів, доступних в системі.
*   **`get_panels`**: Отримує список усіх панелей, доступних в інтерфейсі користувача Home Assistant.
*   **`validate_config`**: Перевіряє, чи є певна частина конфігурації (наприклад, YAML) валідною.

### Media Commands
*   **`media_player_thumbnail`**: Отримує мініатюру для вказаної `media_player` сутності.
*   **`camera_thumbnail`**: Отримує мініатюру для вказаної `camera` сутності.

### Advanced Commands
*   **Target-related Commands**: Група команд (`extract_from_target`, `get_triggers_for_target` та ін.) для роботи з розширеними шаблонами та цілями (targets).
*   **Registry Management**: Велика кількість команд для роботи з реєстрами системи:
    *   `config/entity_registry/*`: Керування сутностями.
    *   `config/device_registry/*`: Керування пристроями.
    *   `config/area_registry/*`: Керування зонами.
*   **`diagnostics/get`**: Отримання діагностичної інформації про пристрій або інтеграцію.
*   **`supported_features`**: Дозволяє клієнту повідомити серверу, які специфічні функції він підтримує, для оптимізації взаємодії.

---
***Disclaimer: The features listed above are planned for future development. Do not implement any of these features without a direct and explicit command.***

## Current Development State

### October 28, 2026 (0.2.0: Production Hardening)

**Goal:** Make the library reliable enough for production use, based on a code review and on tests against a live Home Assistant 2026.9 instance.

**Summary of Changes:**
*   The runner now owns the connection directly: reconnect with exponential backoff, fresh per-connection message ids, subscriptions restored in order, `ping`/`pong` keep-alive, request timeouts, typed `Error`, observable `ConnectionState`.
*   Wire-format fixes: trigger `to`/`from` states, `HH:MM:SS` durations and negative sun offsets, `homeassistant` domain, preset names, compressed state contexts, trigger event payloads.
*   Tolerant parsing: string enums gain `Other(String)`; malformed attributes fall back to untyped.
*   `subscribe_entity` filters server-side through a `state` trigger; callbacks run in order per subscription.
*   Unit tests built from live payload shapes, and end-to-end tests against a mock server (`tests/client.rs`).

See [CHANGELOG.md](CHANGELOG.md) for the full list and the migration guide.

### December 28, 2025 (Trigger Subscriptions)

**Goal:** Implement type-safe subscriptions for Home Assistant automation triggers.

**Summary of Changes:**
A major new feature was added to allow subscribing to automation triggers, providing a powerful way to react to complex events within Home Assistant.

*   **Type-Safe Trigger Structs**: A new `client/triggers.rs` module was created to define strongly-typed Rust structs for all major trigger platforms (e.g., `StateTrigger`, `NumericStateTrigger`, `SunTrigger`). These structs use existing types like `HaState` and `chrono::Duration` to ensure type safety.
*   **Unified Subscription Logic**: The internal subscription handling mechanism was refactored. `HandlerCommand::Subscribe` and the internal `HaEventSubscription` struct now use a new `InternalSubscriptionDetails` enum to differentiate between standard event subscriptions and the new trigger-based subscriptions. This keeps the internal logic clean and centralized.
*   **Ergonomic Trigger Methods**: A set of public, ergonomic methods (`subscribe_state_trigger`, `subscribe_sun_trigger`, etc.) were added to `HAClient`. These methods provide a user-friendly way to create trigger subscriptions with type-safe parameters, hiding the internal complexity.
*   **Event Handling**: A new `events::Event::TriggerFired` variant and a corresponding `TriggerFiredEvent` struct were added to handle incoming trigger events in a type-safe manner. The `handler/incoming.rs` module was updated to deserialize these events and dispatch them to the correct callback.
*   **Documentation and Example**: The `PROJECT_OVERVIEW.md` and `main.rs` were updated to document and demonstrate the new trigger subscription functionality.

**Implementation Complete:**
The client now robustly supports type-safe subscriptions to a wide range of Home Assistant triggers, complete with ergonomic helper methods.

### December 28, 2025 (Ergonomic Subscriptions & Service Call Events)

**Goal:** Implement type-safe subscriptions for `call_service` events and refactor the subscription API to be more ergonomic.

**Summary of Changes:**
A new feature was added and a key API improvement was implemented to enhance user experience.

*   **`call_service` Subscriptions**: Added a new `subscribe_service_calls` method to `HAClient`. This allows users to subscribe to all service call events broadcast on the Home Assistant event bus. The implementation includes a new `events::ServiceCalledEvent` struct and a corresponding `events::Event::ServiceCalled` variant to provide type-safe access to the event data. The `handler/incoming.rs` module was updated to deserialize these new events.
*   **Ergonomic API Refactoring**: The public subscription methods (`subscribe_entity` and the new `subscribe_service_calls`) were refactored to be generic over the callback function. This completely abstracts away the internal `Arc<Box<...>>` boilerplate, allowing users to pass `async` functions or closures directly. This significantly simplifies the public API and improves developer experience.
*   **Documentation Updated**: The project's documentation (`PROJECT_OVERVIEW.md` and `ARCHITECTURE.md`) was updated to reflect these new features and API improvements.

**Implementation Complete:**
The client now supports type-safe subscriptions to service call events and features a much cleaner, more user-friendly API for all subscriptions.

### December 28, 2025 (GetStates Implementation & Code Cleanup)

**Goal:** Implement the `get_states` command and remove all remaining dead code identified in previous steps.

**Summary of Changes:**
A new feature was added and a final cleanup was performed to bring the codebase to a clean, warning-free state.

*   **`get_states` Implemented**: A new public method, `get_states`, was added to `HAClient`. This method uses the standard `get_states` WebSocket command to fetch a `Vec<events::State>` containing the current state of every entity in Home Assistant. The necessary command and result handling logic was implemented in the `client/handler` module.
*   **`get_state` Restored**: The `get_state` method, which uses the efficient one-shot subscription pattern, was restored. The internal logic that was mistakenly removed in a previous step was added back, and the function is now fully operational and included in the `main.rs` demonstration.
*   **Dead Code Removed**: All remaining `dead_code` warnings were eliminated. This involved removing the last pieces of a previous, obsolete implementation for fetching a single state, specifically unused structs and enum variants in the `client/pending.rs` module.
*   **Final Verification**: After these changes, `cargo check` runs cleanly, reporting no errors or warnings, confirming the codebase is now in a stable and clean state.

**Implementation Complete:**
The client now supports fetching all states via `get_states`, correctly implements fetching a single state via `get_state`, and has no remaining dead code from previous refactorings.

### December 28, 2025 (Enum Refactoring with Strum and New Type-Safe Enums)

**Goal:** Enhance type safety and reduce boilerplate by utilizing the `strum` crate for enum string conversions and introducing new type-safe enums for `device_class` and `state_class`.

**Summary of Changes:**
An extensive refactoring was performed to replace manual string conversion logic for various enums with the `strum` crate, and to introduce new strongly-typed enums for commonly used string-based attributes.

*   **`strum` Integration**: The `strum` and `strum_macros` crates were integrated into the project. Many existing enums, including `Domain`, `Service`, `LockState`, `AlarmState`, `CoverState`, `HvacMode`, `FanMode`, `SwingMode`, `PresetMode`, and `UnitOfMeasurement`, were updated to derive `EnumString`, `Display`, and `AsRefStr` (where appropriate) from `strum`. This eliminates boilerplate `from_str`, `to_str`, `to_ha_string`, and `impl fmt::Display` implementations, making the code cleaner, more robust, and less prone to errors due to string mismatches. `strum`'s `serialize_all` and `ascii_case_insensitive` attributes were used to maintain compatibility with Home Assistant's string conventions.
*   **New Type-Safe Enums for Attributes**:
    *   **`DeviceClass`**: A new `enum` was introduced (`client/state/device_class.rs`) to represent the wide range of Home Assistant `device_class` values (e.g., `binary_sensor.battery`, `sensor.temperature`). This replaces `Option<String>` fields with a strongly-typed enum, preventing runtime errors from invalid device class strings.
    *   **`StateClass`**: A new `enum` was introduced (`client/state/state_class.rs`) to represent the defined `state_class` values in Home Assistant (`measurement`, `total`, `total_increasing`). This similarly replaces `Option<String>` fields, enforcing type safety for sensor state classifications.
*   **Updated Attribute Structs**: The `GenericAttributes` and `HistoricalGenericAttributes` structs in `client/events/attributes.rs` were updated to use the new `DeviceClass` and `StateClass` enums, further enhancing the type safety of entity attributes.
*   **Codebase Cleanup**: Removed numerous manual `from_str`, `to_str`, `to_ha_string`, and `impl fmt::Display` blocks, along with several unused `std::fmt` imports, significantly reducing code size and improving readability.

**Implementation Complete:**
This refactoring dramatically improves the type safety and maintainability of the client by leveraging `strum` for enum conversions and introducing new strongly-typed enums for critical Home Assistant attributes. The project compiles and runs successfully with these changes, and future enum implementations will benefit from the established `strum` conventions.

### December 27, 2025 (HaState Refactoring)

**Goal:** Refactor the monolithic `HaState` enum to improve modularity and type safety.

**Summary of Changes:**
A key architectural refactoring was performed on the `HaState` enum to eliminate code duplication and improve type safety.

*   **Problem:** The previous `HaState` implementation mixed states from many different domains (lock, climate, alarm, etc.) into a single large enum. This led to code duplication (e.g., climate states were defined in both `HaState` and `climate::HvacMode`) and made it possible to write logically incorrect code (e.g., comparing a climate entity's state to a lock state).
*   **Solution:** The `HaState` enum was refactored to act as a wrapper that delegates to domain-specific enums.
    *   New modules were created for each domain with a distinct set of states: `state/lock.rs` (`LockState`), `state/alarm.rs` (`AlarmState`), and `state/cover.rs` (`CoverState`).
    *   The `HaState` enum was simplified to hold variants that wrap these specific types (e.g., `HaState::Lock(LockState)`).
    *   The parsing logic in `HaState::from_string` was updated to intelligently delegate parsing to the appropriate domain-specific enum.

**Implementation Complete:**
This change makes the state management architecture significantly more robust, modular, and extensible. It enforces at compile time that states are handled within their correct domain, improving overall code quality. The project compiles and runs successfully with these changes.

**Note on Time Handling:** All time-related data throughout the client, including timestamps, durations, and time zones, are exclusively handled using the `chrono` crate to ensure type safety, consistency, and robust manipulation.

### December 27, 2025 (Attribute Refactoring)

**Goal:** Enhance type safety by replacing `serde_json::Value` with strongly-typed structs and enums for entity attributes.

**Summary of Changes:**
A major refactoring was performed to completely eliminate the use of the generic `serde_json::Value` for handling entity attributes, significantly improving the client's type safety and ergonomics.

*   **Type-Safe Attribute Sets**:
    *   Introduced a new `events::AttributeSet` enum, which acts as a container for different kinds of attributes. It has variants like `AttributeSet::Climate` and a fallback `AttributeSet::Generic`.
    *   Created dedicated attribute structs (e.g., `events::ClimateAttributes`, `events::GenericAttributes`) that contain strongly-typed fields (`current_temperature: Option<f64>`, `friendly_name: Option<String>`, etc.).
*   **Historical Data Parity**: To accurately model the different data available from the history API, parallel structures like `events::HistoricalAttributeSet` and `events::HistoricalGenericAttributes` were created.
*   **`UnitOfMeasurement` Enum**: To further eliminate "magic strings," a new `state::common::UnitOfMeasurement` enum was introduced. This replaces string-based units (like `"V"`, `"°C"`) with enum variants (e.g., `UnitOfMeasurement::Volts`, `UnitOfMeasurement::Celsius`), complete with custom deserialization logic.
*   **Intelligent Deserialization**: The `Deserialize` implementation for `AttributeSet` and `HistoricalAttributeSet` was custom-written to intelligently inspect the incoming JSON and parse it into the correct, domain-specific struct, without needing to know the entity's domain beforehand.
*   **Updated Structs**: The core `events::State` and `events::HistoricalState` structs were updated to use the new `AttributeSet` and `HistoricalAttributeSet` enums, respectively.

**Implementation Complete:**
This refactoring achieves the goal of providing a fully type-safe interface for interacting with entity attributes, making client code more robust and less prone to runtime errors. The project compiles and runs successfully with these changes.

### December 27, 2025 (Additional Refactoring)

**Goal:** Enhance type accuracy for historical data by introducing a dedicated `HistoricalState` struct.

**Summary of Changes:**
To provide a more accurate representation of data received from the Home Assistant history API, a new `HistoricalState` struct was introduced. This addresses the inherent differences in data completeness between live state-changed events and historical snapshots.

*   **Dedicated `HistoricalState` Struct**: A new struct, `events::HistoricalState`, was created to specifically model data from the `history/history_during_period` API endpoint. This struct contains only the fields directly provided by the history API: `entity_id`, `state`, `attributes`, and `last_updated`. It omits `last_changed` and `context`, which are not present in the compact history format.
*   **Specialized Deserialization**: A new internal `events::HistoricalStateWire` struct and corresponding deserialization logic were implemented to correctly parse the history payload, including the `f64` Unix timestamp for `last_updated`.
*   **Updated `get_history` API**: The `HAClient::get_history` public function now returns `Result<Vec<events::HistoricalState>>`, providing users with the precise type for historical data.
*   **`get_state_at_time` Adaptation**: The internal helper `get_state_from_history` was updated to work with `Vec<events::HistoricalState>`. It now filters and finds the relevant historical state based on `last_updated`.
*   **Seamless Conversion**: An `impl From<events::HistoricalState> for events::State` conversion was added. This allows `get_state_at_time` (and other parts of the client that require a full `State` object) to easily convert a `HistoricalState` into the canonical `State` struct, ensuring API consistency. During this conversion, `last_changed` is set to `last_updated`, and `context` is initialized as an empty JSON object, reflecting the data's historical origin.

**Implementation Complete:**
The client now provides a more accurate type representation for historical data while maintaining a convenient and consistent public API. The project compiles and runs successfully with these changes.

### December 27, 2025

**Goal:** Improve time handling by using `chrono` types throughout the client, fix `get_state_at_time` logic, and improve its ergonomics.

**Summary of Changes:**
A major refactoring was performed to make time handling throughout the client more robust, type-safe, and ergonomic. This addressed several issues with the `get_state_at_time` function.

*   **Standardized Timestamps**: All timestamp fields in data structures now exclusively use `chrono::DateTime<Utc>`, eliminating previous inconsistencies and custom string-based handling. This ensures that all internal time representations are strongly typed.
*   **Custom Deserialization**: To support the `chrono` refactoring, custom `deserialize_with` implementations were added for various structs, including `events::State` and internal history items. This allows for correct parsing of Home Assistant's RFC3339 timestamp strings and floating-point Unix timestamps directly into `chrono::DateTime<Utc>` objects. Intermediate deserialization structs and manual parsing logic have been removed.
*   **Robust History Handling**: The history deserialization logic in `client/runner.rs` was significantly improved. It now correctly handles the compact history format from Home Assistant, which uses floating-point Unix timestamps (`lu`), and converts them into `DateTime<Utc>` objects.
*   **Simplified Logic**: The `get_state_from_history` helper function in `client/mod.rs` was simplified to work directly with `DateTime<Utc>` objects, making its comparison logic cleaner and more reliable.
*   **Improved Ergonomics**: The signature of `get_state_at_time` was changed from `Result<Option<State>>` to `Result<State>`. If no state is found in the history, it now returns a descriptive `Err`, which simplifies the calling code by removing a layer of `match` statements.
*   **Timezone-Aware Display**: The example in `main.rs` was updated to convert the resulting UTC timestamp to the user's local timezone for display, making the output more intuitive.

**Implementation Complete:**
The client now handles time in a much more robust and type-safe manner. The `get_state_at_time` function is correct, efficient, and easier to use. The project compiles and runs successfully with these changes.

### December 26, 2025

**Goal:** Fix `get_state_at_time` function, improve its efficiency, and refactor for better code structure.

**Summary of Changes:**
An extensive refactoring of the `get_state_at_time` functionality was performed to fix a critical bug, improve performance, and follow a clearer, more logical structure as requested.

*   **Bug Fix**: Corrected a critical deserialization error that occurred when fetching historical data. The shared `events::State` struct was being used incorrectly for history payloads, which lack an `entity_id`. The logic in `runner.rs` was updated to use a private `HistoryItem` struct for deserialization and then correctly reconstruct the public `events::State` object, ensuring the function now returns a valid result.
*   **Refactoring `get_state_at_time`**:
    *   The function was refactored to be a wrapper around the existing `get_history` function. This removes the need for a dedicated `HandlerCommand::GetStateAtTime` and simplifies the `ClientRunner`'s logic.
    *   A new private helper function, `get_state_from_history`, was introduced in `client/mod.rs` to process the downloaded history and find the single state active at a specific timestamp. This separates network logic from data processing logic.
*   **Efficiency and Flexibility**:
    *   The signature for `get_state_at_time` was updated to accept a `lookback_duration: chrono::Duration`. This gives the user direct control over the time window for fetching history, allowing for significant optimization based on the sensor's update frequency.
    *   The `get_history` call within `get_state_at_time` now uses `significant_changes_only: true` by default, further reducing the data payload from Home Assistant.
*   **Helper Function**: A new public helper, `events::parse_datetime_from_str`, was added to provide a safe way to parse timestamp strings from the API.

**Implementation Complete:**
The `get_state_at_time` function is now robust, efficient, and more flexible. All related compilation errors have been fixed, and the project compiles successfully.

### December 26, 2025

**Goal:** Implement a comprehensive and flexible logging system.

**Summary of Changes:**
A flexible logging system was integrated into the client to provide better visibility into its operations and to aid in debugging.

*   **Technology Stack**: The implementation uses the standard `log` crate as a facade, with `tracing` and `tracing-subscriber` as the backend. The `tracing-log` crate is used to bridge log records to the `tracing` ecosystem.
*   **Configuration**: Logging levels are controlled via the `RUST_LOG` environment variable, thanks to the `env-filter` feature in `tracing-subscriber`. This allows for granular control over log verbosity for the entire application or on a per-module basis.
*   **Log Levels**:
    *   `trace!`: Added to `client/connection.rs` to log the raw text of every incoming and outgoing WebSocket message. This is invaluable for low-level debugging.
    *   `debug!`: Added to `client/mod.rs` and `client/runner.rs` to log key operations like client creation, start/stop, subscriptions, service calls, and internal command handling.
    *   `info!`, `warn!`, `error!`: Existing `tracing` calls were migrated to the `log` facade for consistency. These are used for major lifecycle events and error conditions.
*   **Initialization**: The logging system is initialized at the start of `main()` in `src/main.rs`.

**Implementation Complete:**
The logging system is fully implemented. It enhances debuggability and provides clear insights into the client's internal state and communication with Home Assistant. The project compiles successfully with these changes.

### December 24, 2025

**Goal:** Refactor large code files (`mod.rs`, `handler.rs`, `state.rs`, `context_helpers.rs`) into smaller, more modular, and logically grouped components to improve maintainability and readability.

**Summary of Changes:**
An extensive refactoring was performed to break down large files within the `client` module into more focused sub-modules and files.

*   **`client/mod.rs` and `client/runner.rs`**:
    *   The core `ClientRunner` struct, its `run_loop`, message handling logic (previously in `handler.rs`), and related internal functions were extracted from `client/mod.rs` into a new dedicated file: `client/runner.rs`.
    *   `client/mod.rs` now primarily contains the public `HAClient` handle definition and its public API methods, acting as the entry point to the client library.
    *   The message handling logic (`handle_ha_message`, `handle_event_message`, `handle_result_message`), which was previously in `client/handler.rs`, has been moved and integrated into `client/runner.rs`, leading to the removal of `client/handler.rs`.
*   **`client/state/` module**:
    *   The original `client/state.rs` file was transformed into a module.
    *   Climate-related enums (`HvacMode`, `FanMode`, `SwingMode`, `PresetMode`) were moved from `client/state.rs` into a new sub-module: `client/state/climate.rs`.
    *   `client/state/mod.rs` now contains the general state (`HaState`) and service-related enums (`Domain`, `Service`).
*   **`client/context_helpers/` module**:
    *   The `client/context_helpers.rs` file, which contained various helper methods for `HAClient`, was refactored into a module.
    *   Helper functions were logically grouped by their Home Assistant domain and moved into separate files within the `client/context_helpers/` directory:
        *   `alarm.rs` (Alarm Control Panel helpers)
        *   `climate.rs` (Climate entity helpers)
        *   `cover.rs` (Cover entity helpers)
        *   `homeassistant.rs` (General Home Assistant entity helpers like `turn_on`, `turn_off`, `toggle`)
        *   `input.rs` (Input Number, Text, Select entity helpers)
        *   `lock.rs` (Lock entity helpers)
    *   A `client/context_helpers/mod.rs` was created to declare these new sub-modules.

**Refactoring Complete:**
This refactoring significantly improves the modularity, readability, and maintainability of the `ha_client` codebase. The project now compiles successfully with these structural changes, and all core functionality remains intact.

### December 24, 2025
**Goal:** Eliminate "magic strings" (e.g., "call_service", "auth_ok") from the codebase to improve type safety and robustness.

**Summary of Changes:**
A major refactoring was performed to make the client's communication protocol with Home Assistant fully type-safe.

*   **`client/protocol.rs`**: A new file was introduced to define strongly-typed structs and enums for all WebSocket messages. This includes `OutgoingMsgType` and `IncomingMsgType` enums, as well as structs like `CommandMessage<T>`, `AuthMessage`, `CallServiceMessage` for outgoing commands, and `BaseMessage`, `EventMessage`, `ResultMessage<T>` for incoming messages.
*   **`client/mod.rs`**: The `ClientRunner` was updated to serialize the new typed command structs from `protocol.rs` instead of constructing JSON manually with `serde_json::json!`.
*   **`client/handler.rs`**: The message handling logic was completely rewritten. It now performs a two-step deserialization: first into `BaseMessage` to identify the message type, and then into a specific struct (like `EventMessage` or `ResultMessage`) for safe processing.
*   **`client/connection.rs`**: The authentication handshake was refactored to use the new typed `AuthMessage` and `IncomingMsgType` enum, making it more robust and removing `panic!` calls in favor of returning a `Result::Err`.
*   **`unused.rs`**: The file containing old, obsolete code was removed.

**Refactoring Complete:**
This refactoring successfully eliminates all "magic strings" related to the WebSocket message types, preventing an entire class of runtime errors and making the code easier to maintain and understand. The project compiles successfully and all core functionality remains intact.

### December 24, 2025

**Goal:** Enhance type safety and reduce unnecessary string allocations by replacing string literals with enums and optimizing function signatures.

**Summary of Changes:**
A significant refactoring was performed to further improve the client's type safety and efficiency.

*   **`client/state.rs`**: Introduced new type-safe `Domain` and `Service` enums to represent Home Assistant service domains and specific service calls. These enums replace the use of raw string literals, preventing typos and ensuring compile-time validation of service calls.
*   **`client/subscription.rs`**: The `HandlerCommand::CallService` variant was updated to use the new `Domain` and `Service` enums, ensuring that service commands are type-safe from their inception.
*   **`client/mod.rs`**:
    *   The `HAClient::call_service` function was updated to accept the `Domain` and `Service` enums, and `entity_id` as `Option<&str>`, making the public API cleaner and more robust.
    *   The `ClientRunner::handle_command` logic was adjusted to correctly process the `HandlerCommand::CallService` variant containing the new enums, converting them to their string representations when sending messages to Home Assistant.
    *   The `HAClient::run` function was updated to pass `&str` directly to `WsConnection::new`, avoiding an unnecessary `.to_string()` call.
*   **`client/connection.rs`**: The `WsConnection::new` function now accepts `&str` for `url` and `token`, making its API cleaner. The conversion to owned `String`s now occurs internally for use in the long-running reconnection task.
*   **`client/context_helpers.rs`**: All helper functions (e.g., `set_number`, `turn_on`, `set_hvac_mode`) were updated to use the new `Domain` and `Service` enums when calling `HAClient::call_service`. This change eliminated numerous `.to_string()` calls and `.clone()` operations, significantly reducing string allocations and improving performance.

**Refactoring Complete:**
This refactoring successfully enhances type safety by eliminating string literals for service calls and reduces memory allocations by optimizing string handling across the client's API and internal components. The project compiles successfully and all core functionality remains intact with improved robustness and efficiency.

### December 24, 2025 (Initial)

**Goal:** Refactor the client to support unsubscribing from events and to provide a more robust client lifecycle, including manual start and stop capabilities.

**Summary of Changes:**
An extensive refactoring was performed to move the client to a "Handle/Runner" pattern. The goal was to allow the user to get a client handle (`HAClient`) and call methods on it (`subscribe`, `unsubscribe`, `get_state`) while a background task (`ClientRunner`) handles the event loop.

*   **`client/mod.rs`**: Refactored to separate the `HAClient` handle from the `ClientRunner`. `HAClient` is now a cloneable handle that holds `Arc`-wrapped senders for commands and shutdown signals.
*   **`client/subscription.rs`**: The `EventHandlerContext` was removed. The `EventCallback` type was updated to accept an `HAClient` handle as its first argument, allowing callbacks to make API calls.
*   **`client/handler.rs`**: The message handling logic was updated to construct and pass the `HAClient` handle to the event callbacks.
*   **`client/context_helpers.rs`**: Helper methods (like `set_hvac_mode`) are implemented on `HAClient`, making them available on the main handle and within callbacks.
*   **`main.rs`**: The example code was updated to use the new API, demonstrating how to get a client handle, run the client, subscribe to an entity with a callback, and gracefully shut down.

**Refactoring Complete:**
The refactoring, while architecturally significant, is now complete and the project compiles successfully. The ownership and lifetime issues related to sharing the command channel across the `HAClient` handle and the background `ClientRunner` have been resolved by using `Arc` for the senders. The client now correctly implements the Handle/Runner pattern.