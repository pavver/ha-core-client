# Home Assistant Automation Triggers

This document provides a comprehensive overview of all available triggers in Home Assistant automations. This list can be used as a reference for implementing type-safe trigger subscription helpers in the client.

> **Using these platforms:** the typed helpers cover `state`, `numeric_state`, `sun`, `time`, `time_pattern`, `template` and `zone`. Every platform below can be used through `HAClient::subscribe_trigger_raw`, e.g. `subscribe_trigger_raw(json!({"trigger": "event", "event_type": "my_event"}), cb)`. The platform goes in the `trigger` key (Home Assistant 2024.10+; older releases used `platform`, which current ones still accept). Durations and offsets use the `HH:MM:SS` format.

> **Note on WebSocket API Compatibility:**
> According to the official Home Assistant developer documentation, the `subscribe_trigger` command in the WebSocket API supports the same trigger platforms and syntax as defined here for automations. This list is therefore considered accurate and complete for the purpose of creating WebSocket-based trigger subscriptions.





## Event Trigger

Fires when a specific event is fired on the event bus.

**Trigger:** `event`

### Parameters

| Name         | Optional | Type              | Default | Description                                                   |
|--------------|----------|-------------------|---------|---------------------------------------------------------------|
| `event_type` | No       | String            |         | The name of the event to listen for.                          |
| `event_data` | Yes      | Object (Template) |         | A dictionary to filter event data.                            |
| `context`    | Yes      | Object            |         | A dictionary to filter based on the event's context object.   |

---

## Home Assistant Trigger

Fires on Home Assistant start or shutdown events.

**Trigger:** `homeassistant`

### Parameters

| Name    | Optional | Type   | Default | Description                                       |
|---------|----------|--------|---------|---------------------------------------------------|
| `event` | No       | String |         | The event to trigger on: `start` or `shutdown`.   |

---





## Geolocation Trigger

Fires when an entity is in proximity to a location.

**Trigger:** `geo_location`

### Parameters

| Name      | Optional | Type   | Default | Description                                                 |
|-----------|----------|--------|---------|-------------------------------------------------------------|
| `source`  | No       | String |         | The source of the geolocation data (e.g., a GeoRSS feed).   |
| `zone`    | No       | String |         | The `zone` entity ID to monitor for proximity.              |
| `event`   | No       | String | `enter` | The event to trigger on: `enter` or `leave`.                |

---

## MQTT Trigger

Fires when a message is received on an MQTT topic.

**Trigger:** `mqtt`

### Parameters

| Name           | Optional | Type              | Default | Description                                                               |
|----------------|----------|-------------------|---------|---------------------------------------------------------------------------|
| `topic`        | No       | String            |         | The MQTT topic to subscribe to.                                           |
| `payload`      | Yes      | String            |         | The payload that must be present for the trigger to fire.                 |
| `value_template` | Yes      | String (Template) |         | A template to extract a value from the payload.                           |
| `encoding`     | Yes      | String            | `utf-8` | The encoding of the payload.                                              |
| `qos`          | Yes      | Number (0-2)      | 0       | The Quality of Service level.                                             |

---

## Device Trigger

Fires when a device-specific event occurs. This is highly dependent on the device and integration.

**Trigger:** `device`

### Parameters

| Name         | Optional | Type   | Default | Description                                                   |
|--------------|----------|--------|---------|---------------------------------------------------------------|
| `device_id`  | No       | String |         | The unique ID of the device.                                  |
| `domain`     | No       | String |         | The domain of the device (e.g., `binary_sensor`).             |
| `entity_id`  | No       | String |         | The entity ID associated with the device action.              |
| `type`       | No       | String |         | The specific trigger type for the device (e.g., `turned_on`).   |
| `subtype`    | Yes      | String |         | A more specific subtype for the trigger (e.g., `button_1`).   |
| `for`        | Yes      | Time   |         | The duration the condition must be met.                       |

---

## Tag Trigger

Fires when a Home Assistant tag is scanned.

**Trigger:** `tag`

### Parameters

| Name      | Optional | Type                     | Default | Description                   |
|-----------|----------|--------------------------|---------|-------------------------------|
| `tag_id`  | No       | String / List of Strings |         | The ID of the tag to react to. |
| `device_id`| Yes      | String / List of Strings |         | The device that scanned the tag.|

---

## Webhook Trigger

Fires when an HTTP request is made to a specific webhook endpoint.

**Trigger:** `webhook`

### Parameters

| Name        | Optional | Type   | Default | Description                                                                  |
|-------------|----------|--------|---------|------------------------------------------------------------------------------|
| `webhook_id`| No       | String |         | The unique ID for the webhook URL.                                           |
| `local_only`| Yes      | Boolean| `false` | If `true`, only requests from the local network are allowed.                   |
| `allowed_methods`| Yes | List of Strings | `POST` | A list of allowed HTTP methods (e.g., `GET`, `POST`, `PUT`). |

---

## Calendar Trigger

Fires based on an event in a calendar entity.

**Trigger:** `calendar`

### Parameters

| Name        | Optional | Type   | Default | Description                                                                  |
|-------------|----------|--------|---------|------------------------------------------------------------------------------|
| `entity_id` | No       | String |         | The calendar entity ID.                                                      |
| `event`     | No       | String | `start` | The event to trigger on: `start` or `end`.                                   |
| `offset`    | Yes      | Time   | 0       | An offset from the event's start/end time.                                   |

---

## Persistent Notification Trigger

Fires when a persistent notification is updated.

**Trigger:** `persistent_notification`

### Parameters

| Name             | Optional | Type   | Default | Description                                                                                               |
|------------------|----------|--------|---------|-----------------------------------------------------------------------------------------------------------|
| `notification_id`| Yes      | String |         | The ID of the notification to monitor. If omitted, triggers for any notification.                         |
| `update_type`    | Yes      | String | `added` | The type of update to trigger on: `added`, `removed`, or `updated`. Can also be a list, e.g. `[added, removed]` |

---

## Sentence Trigger

Fires when a command with a specific sentence is processed. Requires a conversation integration.

**Trigger:** `sentence`

### Parameters

| Name     | Optional | Type                     | Default | Description                                           |
|----------|----------|--------------------------|---------|-------------------------------------------------------|
| `sentence` | No       | String / List of Strings |         | The sentence(s) to match. Can include template slots. |