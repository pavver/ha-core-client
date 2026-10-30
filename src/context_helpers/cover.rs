use crate::Result;
use serde_json::json;

use crate::{
    state::{Domain, Service},
    HAClient, Target,
};

impl HAClient {
    // --- Cover Domain Helpers ---

    pub async fn open_cover(&self, target: impl Into<Target>) -> Result<()> {
        self.call_service(
            Domain::Cover,
            Service::OpenCover,
            target,
            (),
        )
        .await
    }

    pub async fn close_cover(&self, target: impl Into<Target>) -> Result<()> {
        self.call_service(
            Domain::Cover,
            Service::CloseCover,
            target,
            (),
        )
        .await
    }

    pub async fn stop_cover(&self, target: impl Into<Target>) -> Result<()> {
        self.call_service(
            Domain::Cover,
            Service::StopCover,
            target,
            (),
        )
        .await
    }

    pub async fn set_cover_position(&self, target: impl Into<Target>, position: u8) -> Result<()> {
        let service_data = json!({ "position": position });
        self.call_service(
            Domain::Cover,
            Service::SetCoverPosition,
            target,
            service_data,
        )
        .await
    }

    pub async fn set_cover_tilt_position(&self, target: impl Into<Target>, tilt_position: u8) -> Result<()> {
        let service_data = json!({ "tilt_position": tilt_position });
        self.call_service(
            Domain::Cover,
            Service::SetCoverTiltPosition,
            target,
            service_data,
        )
        .await
    }
}
