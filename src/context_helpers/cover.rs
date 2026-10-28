use crate::Result;
use serde_json::json;

use crate::{
    state::{Domain, Service},
    HAClient,
};

impl HAClient {
    // --- Cover Domain Helpers ---

    pub async fn open_cover(&self, entity_id: &str) -> Result<()> {
        self.call_service(
            Domain::Cover,
            Service::OpenCover,
            Some(entity_id),
            None,
        )
        .await
    }

    pub async fn close_cover(&self, entity_id: &str) -> Result<()> {
        self.call_service(
            Domain::Cover,
            Service::CloseCover,
            Some(entity_id),
            None,
        )
        .await
    }

    pub async fn stop_cover(&self, entity_id: &str) -> Result<()> {
        self.call_service(
            Domain::Cover,
            Service::StopCover,
            Some(entity_id),
            None,
        )
        .await
    }

    pub async fn set_cover_position(&self, entity_id: &str, position: u8) -> Result<()> {
        let service_data = json!({ "position": position });
        self.call_service(
            Domain::Cover,
            Service::SetCoverPosition,
            Some(entity_id),
            Some(service_data),
        )
        .await
    }

    pub async fn set_cover_tilt_position(&self, entity_id: &str, tilt_position: u8) -> Result<()> {
        let service_data = json!({ "tilt_position": tilt_position });
        self.call_service(
            Domain::Cover,
            Service::SetCoverTiltPosition,
            Some(entity_id),
            Some(service_data),
        )
        .await
    }
}
