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
            entity_id,
            (),
        )
        .await
    }

    pub async fn close_cover(&self, entity_id: &str) -> Result<()> {
        self.call_service(
            Domain::Cover,
            Service::CloseCover,
            entity_id,
            (),
        )
        .await
    }

    pub async fn stop_cover(&self, entity_id: &str) -> Result<()> {
        self.call_service(
            Domain::Cover,
            Service::StopCover,
            entity_id,
            (),
        )
        .await
    }

    pub async fn set_cover_position(&self, entity_id: &str, position: u8) -> Result<()> {
        let service_data = json!({ "position": position });
        self.call_service(
            Domain::Cover,
            Service::SetCoverPosition,
            entity_id,
            service_data,
        )
        .await
    }

    pub async fn set_cover_tilt_position(&self, entity_id: &str, tilt_position: u8) -> Result<()> {
        let service_data = json!({ "tilt_position": tilt_position });
        self.call_service(
            Domain::Cover,
            Service::SetCoverTiltPosition,
            entity_id,
            service_data,
        )
        .await
    }
}
