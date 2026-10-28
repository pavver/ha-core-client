use crate::{Error, Result};

use crate::{
    state::{Domain, Service},
    HAClient,
};

impl HAClient {
    /// Calls `service` in the entity's own domain (e.g. `light.turn_on` for `light.kitchen`).
    async fn call_entity_service(&self, entity_id: &str, service: Service) -> Result<()> {
        let domain = Domain::of_entity(entity_id)
            .ok_or_else(|| Error::InvalidInput(format!("invalid entity_id: {entity_id}")))?;
        self.call_service(domain, service, Some(entity_id), None).await
    }

    /// Turns on an entity (switch, light, fan, …).
    pub async fn turn_on(&self, entity_id: &str) -> Result<()> {
        self.call_entity_service(entity_id, Service::TurnOn).await
    }

    /// Turns off an entity (switch, light, fan, …).
    pub async fn turn_off(&self, entity_id: &str) -> Result<()> {
        self.call_entity_service(entity_id, Service::TurnOff).await
    }

    /// Toggles an entity (switch, light, fan, …).
    pub async fn toggle(&self, entity_id: &str) -> Result<()> {
        self.call_entity_service(entity_id, Service::Toggle).await
    }
}
