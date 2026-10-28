use crate::Result;
use serde_json::json;

use crate::{
    state::{Domain, Service},
    HAClient,
};

impl HAClient {
    // --- Alarm Control Panel Domain Helpers ---

    /// Helper to call alarm services, optionally with a code.
    async fn call_alarm_service(
        &self,
        entity_id: &str,
        service: Service,
        code: Option<&str>,
    ) -> Result<()> {
        let service_data = code.map(|c| json!({ "code": c }));
        self.call_service(
            Domain::AlarmControlPanel,
            service,
            entity_id,
            service_data,
        )
        .await
    }

    pub async fn alarm_disarm(&self, entity_id: &str, code: Option<&str>) -> Result<()> {
        self.call_alarm_service(entity_id, Service::AlarmDisarm, code)
            .await
    }

    pub async fn alarm_arm_home(&self, entity_id: &str, code: Option<&str>) -> Result<()> {
        self.call_alarm_service(entity_id, Service::AlarmArmHome, code)
            .await
    }

    pub async fn alarm_arm_away(&self, entity_id: &str, code: Option<&str>) -> Result<()> {
        self.call_alarm_service(entity_id, Service::AlarmArmAway, code)
            .await
    }

    pub async fn alarm_arm_night(&self, entity_id: &str, code: Option<&str>) -> Result<()> {
        self.call_alarm_service(entity_id, Service::AlarmArmNight, code)
            .await
    }

    pub async fn alarm_arm_vacation(&self, entity_id: &str, code: Option<&str>) -> Result<()> {
        self.call_alarm_service(entity_id, Service::AlarmArmVacation, code)
            .await
    }
}
