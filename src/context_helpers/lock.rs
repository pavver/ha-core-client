use crate::Result;
use serde_json::json;

use crate::{
    state::{Domain, Service},
    HAClient, Target,
};

impl HAClient {
    // --- Lock Domain Helpers ---

    /// Helper to call lock services, optionally with a code.
    async fn call_lock_service(
        &self,
        target: impl Into<Target>,
        service: Service,
        code: Option<&str>,
    ) -> Result<()> {
        let service_data = code.map(|c| json!({ "code": c }));
        self.call_service(
            Domain::Lock,
            service,
            target,
            service_data,
        )
        .await
    }

    pub async fn lock(&self, target: impl Into<Target>, code: Option<&str>) -> Result<()> {
        self.call_lock_service(target, Service::Lock, code).await
    }

    pub async fn unlock(&self, target: impl Into<Target>, code: Option<&str>) -> Result<()> {
        self.call_lock_service(target, Service::Unlock, code).await
    }
}
