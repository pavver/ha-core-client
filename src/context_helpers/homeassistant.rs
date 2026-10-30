use crate::{Error, Result};

use crate::{
    state::{Domain, Service},
    HAClient, Target,
};

/// The domain whose `turn_on`/`turn_off`/`toggle` to call for `target`: the shared domain if it
/// is only entity ids of one domain, otherwise `homeassistant`, which handles any target.
fn service_domain(target: &Target) -> Result<Domain> {
    let domains = target
        .entity_id
        .iter()
        .map(|id| Domain::of_entity(id).ok_or_else(|| Error::InvalidInput(format!("invalid entity_id: {id}"))))
        .collect::<Result<Vec<_>>>()?;
    let entities_only = target.device_id.is_empty()
        && target.area_id.is_empty()
        && target.floor_id.is_empty()
        && target.label_id.is_empty();
    match domains.split_first() {
        Some((first, rest)) if entities_only && rest.iter().all(|d| d == first) => Ok(first.clone()),
        _ => Ok(Domain::HomeAssistant),
    }
}

impl HAClient {
    /// Calls `service` in the target's domain (e.g. `light.turn_on` for `light.kitchen`), or in
    /// `homeassistant` if the target is not entity ids of one domain.
    async fn call_target_service(&self, target: impl Into<Target>, service: Service) -> Result<()> {
        let target = target.into();
        let domain = service_domain(&target)?;
        self.call_service(domain, service, target, ()).await
    }

    /// Turns on entities (switch, light, fan, …). Entity ids of one domain call that domain's
    /// service; areas, devices, floors, labels or mixed domains call `homeassistant.turn_on`.
    pub async fn turn_on(&self, target: impl Into<Target>) -> Result<()> {
        self.call_target_service(target, Service::TurnOn).await
    }

    /// Turns off entities (switch, light, fan, …); see [`turn_on`](Self::turn_on).
    pub async fn turn_off(&self, target: impl Into<Target>) -> Result<()> {
        self.call_target_service(target, Service::TurnOff).await
    }

    /// Toggles entities (switch, light, fan, …); see [`turn_on`](Self::turn_on).
    pub async fn toggle(&self, target: impl Into<Target>) -> Result<()> {
        self.call_target_service(target, Service::Toggle).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_is_shared_or_homeassistant() {
        assert_eq!(service_domain(&"light.a".into()).unwrap(), Domain::Light);
        assert_eq!(service_domain(&["light.a", "light.b"].into()).unwrap(), Domain::Light);
        assert_eq!(service_domain(&["light.a", "switch.b"].into()).unwrap(), Domain::HomeAssistant);
        assert_eq!(service_domain(&Target::area("kitchen")).unwrap(), Domain::HomeAssistant);
        assert_eq!(service_domain(&Target::area("kitchen").and_entity("light.a")).unwrap(), Domain::HomeAssistant);
        assert_eq!(service_domain(&Target::device("d")).unwrap(), Domain::HomeAssistant);
        assert_eq!(service_domain(&Target::new()).unwrap(), Domain::HomeAssistant);
        for bad in [Target::entity("nope"), Target::entities(["light.a", "nope"]), Target::area("k").and_entity("nope")] {
            assert!(matches!(service_domain(&bad), Err(Error::InvalidInput(_))), "{bad:?}");
        }
    }
}
