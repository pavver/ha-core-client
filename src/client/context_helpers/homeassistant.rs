use anyhow::Result;

use crate::client::{
    state::{Domain, Service},
    HAClient,
};

impl HAClient {
    /// Turns on an entity (switch, light, etc.).
    pub async fn turn_on(&self, entity_id: &str) -> Result<()> {
        self.call_service(
            Domain::HomeAssistant,
            Service::TurnOn,
            Some(entity_id),
            None,
        )
        .await
    }

    /// Turns off an entity (switch, light, etc.).
    pub async fn turn_off(&self, entity_id: &str) -> Result<()> {
        self.call_service(
            Domain::HomeAssistant,
            Service::TurnOff,
            Some(entity_id),
            None,
        )
        .await
    }

    /// Toggles an entity (switch, light, etc.).
    pub async fn toggle(&self, entity_id: &str) -> Result<()> {
        self.call_service(
            Domain::HomeAssistant,
            Service::Toggle,
            Some(entity_id),
            None,
        )
        .await
    }
}
