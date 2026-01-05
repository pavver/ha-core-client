use anyhow::Result;
use serde_json::json;

use crate::{
    client::state::{Domain, Service},
    client::HAClient,
};

impl HAClient {
    // --- Lock Domain Helpers ---

    /// Helper to call lock services, optionally with a code.
    async fn call_lock_service(
        &self,
        entity_id: &str,
        service: Service,
        code: Option<&str>,
    ) -> Result<()> {
        let service_data = code.map(|c| json!({ "code": c }));
        self.call_service(
            Domain::Lock,
            service,
            Some(entity_id),
            service_data,
        )
        .await
    }

    pub async fn lock(&self, entity_id: &str, code: Option<&str>) -> Result<()> {
        self.call_lock_service(entity_id, Service::Lock, code).await
    }

    pub async fn unlock(&self, entity_id: &str, code: Option<&str>) -> Result<()> {
        self.call_lock_service(entity_id, Service::Unlock, code).await
    }
}
