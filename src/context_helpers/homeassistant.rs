use anyhow::{anyhow, Result};
use std::str::FromStr;

use crate::{
    state::{Domain, Service},
    HAClient,
};

impl HAClient {
    /// Turns on an entity (switch, light, etc.).
    pub async fn turn_on(&self, entity_id: &str) -> Result<()> {
        let domain_str = entity_id
            .split('.')
            .next()
            .ok_or_else(|| anyhow!("Invalid entity_id: {}", entity_id))?;
        let domain =
            Domain::from_str(domain_str).map_err(|_| anyhow!("Unknown domain: {}", domain_str))?;

        self.call_service(domain, Service::TurnOn, Some(entity_id), None)
            .await
    }

    /// Turns off an entity (switch, light, etc.).
    pub async fn turn_off(&self, entity_id: &str) -> Result<()> {
        let domain_str = entity_id
            .split('.')
            .next()
            .ok_or_else(|| anyhow!("Invalid entity_id: {}", entity_id))?;
        let domain =
            Domain::from_str(domain_str).map_err(|_| anyhow!("Unknown domain: {}", domain_str))?;

        self.call_service(domain, Service::TurnOff, Some(entity_id), None)
            .await
    }

    /// Toggles an entity (switch, light, etc.).
    pub async fn toggle(&self, entity_id: &str) -> Result<()> {
        let domain_str = entity_id
            .split('.')
            .next()
            .ok_or_else(|| anyhow!("Invalid entity_id: {}", entity_id))?;
        let domain =
            Domain::from_str(domain_str).map_err(|_| anyhow!("Unknown domain: {}", domain_str))?;

        self.call_service(domain, Service::Toggle, Some(entity_id), None)
            .await
    }
}
