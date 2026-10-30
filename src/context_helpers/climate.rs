use crate::Result;
use serde_json::json;

use crate::{
    state::{
        climate::{FanMode, HvacMode, PresetMode, SwingMode},
        Domain, Service,
    },
    HAClient, Target,
};

impl HAClient {
    // --- Climate Domain Helpers ---

    /// Sets the HVAC mode of a `climate` entity.
    pub async fn set_hvac_mode(&self, target: impl Into<Target>, hvac_mode: HvacMode) -> Result<()> {
        let service_data = json!({ "hvac_mode": hvac_mode.to_string() });
        self.call_service(
            Domain::Climate,
            Service::SetHvacMode,
            target,
            service_data,
        )
        .await
    }

    /// Sets the target temperature of a `climate` entity.
    pub async fn set_temperature(&self, target: impl Into<Target>, temperature: f64) -> Result<()> {
        let service_data = json!({ "temperature": temperature });
        self.call_service(
            Domain::Climate,
            Service::SetTemperature,
            target,
            service_data,
        )
        .await
    }

    /// Sets the target temperature range of a `climate` entity, for `heat_cool` mode.
    pub async fn set_temperature_range(&self, target: impl Into<Target>, low: f64, high: f64) -> Result<()> {
        let service_data = json!({ "target_temp_low": low, "target_temp_high": high });
        self.call_service(
            Domain::Climate,
            Service::SetTemperature,
            target,
            service_data,
        )
        .await
    }

    /// Sets the fan mode of a `climate` entity.
    pub async fn set_fan_mode(&self, target: impl Into<Target>, fan_mode: FanMode) -> Result<()> {
        let service_data = json!({ "fan_mode": fan_mode.to_string() });
        self.call_service(
            Domain::Climate,
            Service::SetFanMode,
            target,
            service_data,
        )
        .await
    }

    /// Sets the preset mode of a `climate` entity.
    pub async fn set_preset_mode(&self, target: impl Into<Target>, preset_mode: PresetMode) -> Result<()> {
        let service_data = json!({ "preset_mode": preset_mode.to_string() });
        self.call_service(
            Domain::Climate,
            Service::SetPresetMode,
            target,
            service_data,
        )
        .await
    }

    /// Sets the swing mode of a `climate` entity.
    pub async fn set_swing_mode(&self, target: impl Into<Target>, swing_mode: SwingMode) -> Result<()> {
        let service_data = json!({ "swing_mode": swing_mode.to_string() });
        self.call_service(
            Domain::Climate,
            Service::SetSwingMode,
            target,
            service_data,
        )
        .await
    }
}
