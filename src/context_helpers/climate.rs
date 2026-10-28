use crate::Result;
use serde_json::json;

use crate::{
    state::{
        climate::{FanMode, HvacMode, PresetMode, SwingMode},
        Domain, Service,
    },
    HAClient,
};

impl HAClient {
    // --- Climate Domain Helpers ---

    /// Sets the HVAC mode of a `climate` entity.
    pub async fn set_hvac_mode(&self, entity_id: &str, hvac_mode: HvacMode) -> Result<()> {
        let service_data = json!({ "hvac_mode": hvac_mode.to_string() });
        self.call_service(
            Domain::Climate,
            Service::SetHvacMode,
            Some(entity_id),
            Some(service_data),
        )
        .await
    }

    /// Sets the target temperature of a `climate` entity.
    pub async fn set_temperature(&self, entity_id: &str, temperature: f64) -> Result<()> {
        let service_data = json!({ "temperature": temperature });
        self.call_service(
            Domain::Climate,
            Service::SetTemperature,
            Some(entity_id),
            Some(service_data),
        )
        .await
    }

    /// Sets the fan mode of a `climate` entity.
    pub async fn set_fan_mode(&self, entity_id: &str, fan_mode: FanMode) -> Result<()> {
        let service_data = json!({ "fan_mode": fan_mode.to_string() });
        self.call_service(
            Domain::Climate,
            Service::SetFanMode,
            Some(entity_id),
            Some(service_data),
        )
        .await
    }

    /// Sets the preset mode of a `climate` entity.
    pub async fn set_preset_mode(&self, entity_id: &str, preset_mode: PresetMode) -> Result<()> {
        let service_data = json!({ "preset_mode": preset_mode.to_string() });
        self.call_service(
            Domain::Climate,
            Service::SetPresetMode,
            Some(entity_id),
            Some(service_data),
        )
        .await
    }

    /// Sets the swing mode of a `climate` entity.
    pub async fn set_swing_mode(&self, entity_id: &str, swing_mode: SwingMode) -> Result<()> {
        let service_data = json!({ "swing_mode": swing_mode.to_string() });
        self.call_service(
            Domain::Climate,
            Service::SetSwingMode,
            Some(entity_id),
            Some(service_data),
        )
        .await
    }
}
