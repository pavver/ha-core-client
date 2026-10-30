use crate::Result;
use serde_json::json;

use crate::{
    state::{Domain, Service},
    HAClient, Target,
};

impl HAClient {
    // --- Alarm Control Panel Domain Helpers ---

    /// Helper to call alarm services, optionally with a code.
    async fn call_alarm_service(
        &self,
        target: impl Into<Target>,
        service: Service,
        code: Option<&str>,
    ) -> Result<()> {
        let service_data = code.map(|c| json!({ "code": c }));
        self.call_service(
            Domain::AlarmControlPanel,
            service,
            target,
            service_data,
        )
        .await
    }

    pub async fn alarm_disarm(&self, target: impl Into<Target>, code: Option<&str>) -> Result<()> {
        self.call_alarm_service(target, Service::AlarmDisarm, code)
            .await
    }

    pub async fn alarm_arm_home(&self, target: impl Into<Target>, code: Option<&str>) -> Result<()> {
        self.call_alarm_service(target, Service::AlarmArmHome, code)
            .await
    }

    pub async fn alarm_arm_away(&self, target: impl Into<Target>, code: Option<&str>) -> Result<()> {
        self.call_alarm_service(target, Service::AlarmArmAway, code)
            .await
    }

    pub async fn alarm_arm_night(&self, target: impl Into<Target>, code: Option<&str>) -> Result<()> {
        self.call_alarm_service(target, Service::AlarmArmNight, code)
            .await
    }

    pub async fn alarm_arm_vacation(&self, target: impl Into<Target>, code: Option<&str>) -> Result<()> {
        self.call_alarm_service(target, Service::AlarmArmVacation, code)
            .await
    }
}
