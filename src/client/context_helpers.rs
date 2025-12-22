use super::{
    state::{FanMode, PresetMode, SwingMode},
    subscription::EventHandlerContext,
};
use anyhow::Result;
use serde_json::json;

impl EventHandlerContext {
    /// Helper function to simplify setting the value of an `input_number` entity.
    pub async fn set_number(&self, entity_id: &str, value: f64) -> Result<()> {
        let full_entity_id = if entity_id.contains('.') {
            entity_id.to_string()
        } else {
            format!("input_number.{}", entity_id)
        };
        let service_data = json!({ "value": value });
        self.call_service(
            "input_number".to_string(),
            "set_value".to_string(),
            Some(full_entity_id),
            Some(service_data),
        )
        .await
    }

    /// Turns on an entity (switch, light, etc.).
    pub async fn turn_on(&self, entity_id: &str) -> Result<()> {
        self.call_service(
            "homeassistant".to_string(),
            "turn_on".to_string(),
            Some(entity_id.to_string()),
            None,
        )
        .await
    }

    /// Turns off an entity (switch, light, etc.).
    pub async fn turn_off(&self, entity_id: &str) -> Result<()> {
        self.call_service(
            "homeassistant".to_string(),
            "turn_off".to_string(),
            Some(entity_id.to_string()),
            None,
        )
        .await
    }

    /// Toggles an entity (switch, light, etc.).
    pub async fn toggle(&self, entity_id: &str) -> Result<()> {
        self.call_service(
            "homeassistant".to_string(),
            "toggle".to_string(),
            Some(entity_id.to_string()),
            None,
        )
        .await
    }

    /// Sets the value of an `input_text` entity.
    pub async fn set_text(&self, entity_id: &str, value: &str) -> Result<()> {
        let full_entity_id = if entity_id.contains('.') {
            entity_id.to_string()
        } else {
            format!("input_text.{}", entity_id)
        };
        let service_data = json!({ "value": value });
        self.call_service(
            "input_text".to_string(),
            "set_value".to_string(),
            Some(full_entity_id),
            Some(service_data),
        )
        .await
    }

    /// Selects an option for an `input_select` entity.
    pub async fn select_option(&self, entity_id: &str, option: &str) -> Result<()> {
        let full_entity_id = if entity_id.contains('.') {
            entity_id.to_string()
        } else {
            format!("input_select.{}", entity_id)
        };
        let service_data = json!({ "option": option });
        self.call_service(
            "input_select".to_string(),
            "select_option".to_string(),
            Some(full_entity_id),
            Some(service_data),
        )
        .await
    }

    // --- Climate Domain Helpers ---

    /// Sets the HVAC mode of a `climate` entity.
    pub async fn set_hvac_mode(&self, entity_id: &str, hvac_mode: super::state::HvacMode) -> Result<()> {
        let service_data = json!({ "hvac_mode": hvac_mode.to_ha_string() });
        self.call_service(
            "climate".to_string(),
            "set_hvac_mode".to_string(),
            Some(entity_id.to_string()),
            Some(service_data),
        )
        .await
    }

    /// Sets the target temperature of a `climate` entity.
    pub async fn set_temperature(&self, entity_id: &str, temperature: f64) -> Result<()> {
        let service_data = json!({ "temperature": temperature });
        self.call_service(
            "climate".to_string(),
            "set_temperature".to_string(),
            Some(entity_id.to_string()),
            Some(service_data),
        )
        .await
    }

    /// Sets the fan mode of a `climate` entity.
    pub async fn set_fan_mode(&self, entity_id: &str, fan_mode: FanMode) -> Result<()> {
        let service_data = json!({ "fan_mode": fan_mode.to_ha_string() });
        self.call_service(
            "climate".to_string(),
            "set_fan_mode".to_string(),
            Some(entity_id.to_string()),
            Some(service_data),
        )
        .await
    }

    /// Sets the preset mode of a `climate` entity.
    pub async fn set_preset_mode(&self, entity_id: &str, preset_mode: PresetMode) -> Result<()> {
        let service_data = json!({ "preset_mode": preset_mode.to_ha_string() });
        self.call_service(
            "climate".to_string(),
            "set_preset_mode".to_string(),
            Some(entity_id.to_string()),
            Some(service_data),
        )
        .await
    }

    /// Sets the swing mode of a `climate` entity.
    pub async fn set_swing_mode(&self, entity_id: &str, swing_mode: SwingMode) -> Result<()> {
        let service_data = json!({ "swing_mode": swing_mode.to_ha_string() });
        self.call_service(
            "climate".to_string(),
            "set_swing_mode".to_string(),
            Some(entity_id.to_string()),
            Some(service_data),
        )
        .await
    }

    // --- Alarm Control Panel Domain Helpers ---

    /// Helper to call alarm services, optionally with a code.
    async fn call_alarm_service(
        &self,
        entity_id: &str,
        service: &str,
        code: Option<&str>,
    ) -> Result<()> {
        let service_data = code.map(|c| json!({ "code": c }));
        self.call_service(
            "alarm_control_panel".to_string(),
            service.to_string(),
            Some(entity_id.to_string()),
            service_data,
        )
        .await
    }

    pub async fn alarm_disarm(&self, entity_id: &str, code: Option<&str>) -> Result<()> {
        self.call_alarm_service(entity_id, "alarm_disarm", code)
            .await
    }

    pub async fn alarm_arm_home(&self, entity_id: &str, code: Option<&str>) -> Result<()> {
        self.call_alarm_service(entity_id, "alarm_arm_home", code)
            .await
    }

    pub async fn alarm_arm_away(&self, entity_id: &str, code: Option<&str>) -> Result<()> {
        self.call_alarm_service(entity_id, "alarm_arm_away", code)
            .await
    }

    pub async fn alarm_arm_night(&self, entity_id: &str, code: Option<&str>) -> Result<()> {
        self.call_alarm_service(entity_id, "alarm_arm_night", code)
            .await
    }

    pub async fn alarm_arm_vacation(&self, entity_id: &str, code: Option<&str>) -> Result<()> {
        self.call_alarm_service(entity_id, "alarm_arm_vacation", code)
            .await
    }

    // --- Lock Domain Helpers ---

    /// Helper to call lock services, optionally with a code.
    async fn call_lock_service(
        &self,
        entity_id: &str,
        service: &str,
        code: Option<&str>,
    ) -> Result<()> {
        let service_data = code.map(|c| json!({ "code": c }));
        self.call_service(
            "lock".to_string(),
            service.to_string(),
            Some(entity_id.to_string()),
            service_data,
        )
        .await
    }

    pub async fn lock(&self, entity_id: &str, code: Option<&str>) -> Result<()> {
        self.call_lock_service(entity_id, "lock", code).await
    }

    pub async fn unlock(&self, entity_id: &str, code: Option<&str>) -> Result<()> {
        self.call_lock_service(entity_id, "unlock", code).await
    }

    // --- Cover Domain Helpers ---

    pub async fn open_cover(&self, entity_id: &str) -> Result<()> {
        self.call_service(
            "cover".to_string(),
            "open_cover".to_string(),
            Some(entity_id.to_string()),
            None,
        )
        .await
    }

    pub async fn close_cover(&self, entity_id: &str) -> Result<()> {
        self.call_service(
            "cover".to_string(),
            "close_cover".to_string(),
            Some(entity_id.to_string()),
            None,
        )
        .await
    }

    pub async fn stop_cover(&self, entity_id: &str) -> Result<()> {
        self.call_service(
            "cover".to_string(),
            "stop_cover".to_string(),
            Some(entity_id.to_string()),
            None,
        )
        .await
    }

    pub async fn set_cover_position(&self, entity_id: &str, position: u8) -> Result<()> {
        let service_data = json!({ "position": position });
        self.call_service(
            "cover".to_string(),
            "set_cover_position".to_string(),
            Some(entity_id.to_string()),
            Some(service_data),
        )
        .await
    }

    pub async fn set_cover_tilt_position(&self, entity_id: &str, tilt_position: u8) -> Result<()> {
        let service_data = json!({ "tilt_position": tilt_position });
        self.call_service(
            "cover".to_string(),
            "set_cover_tilt_position".to_string(),
            Some(entity_id.to_string()),
            Some(service_data),
        )
        .await
    }
}
