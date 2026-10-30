use crate::{Error, Result};
use serde_json::json;

use crate::{
    state::{Domain, Service},
    HAClient, Target,
};

/// Resolves the target and the domain whose service to call: bare object ids (`"target"`) get
/// `default` as their domain, full ids keep their own (`number.x`). The domain is the one all
/// entity ids share; a target without entity ids (an area, …) uses `default`. Entity ids of
/// different domains are an [`Error::InvalidInput`], as no single service fits them.
fn resolve(target: impl Into<Target>, default: Domain) -> Result<(Domain, Target)> {
    let mut target = target.into();
    let mut domain: Option<Domain> = None;
    for id in &mut target.entity_id {
        let id_domain = match Domain::of_entity(id) {
            Some(own) => own,
            None => {
                *id = format!("{default}.{id}");
                default.clone()
            }
        };
        match &domain {
            Some(shared) if *shared != id_domain => {
                return Err(Error::InvalidInput(format!("entity ids of different domains: {shared} and {id_domain}")));
            }
            _ => domain = Some(id_domain),
        }
    }
    Ok((domain.unwrap_or(default), target))
}

impl HAClient {
    /// Sets the value of `input_number` or `number` entities. A bare object id
    /// (`"target_temp"`) means `input_number.target_temp`.
    pub async fn set_number(&self, target: impl Into<Target>, value: f64) -> Result<()> {
        let (domain, target) = resolve(target, Domain::InputNumber)?;
        let service_data = json!({ "value": value });
        self.call_service(domain, Service::SetValue, target, service_data).await
    }

    /// Sets the value of `input_text` or `text` entities. A bare object id means `input_text.<id>`.
    pub async fn set_text(&self, target: impl Into<Target>, value: &str) -> Result<()> {
        let (domain, target) = resolve(target, Domain::InputText)?;
        let service_data = json!({ "value": value });
        self.call_service(domain, Service::SetValue, target, service_data).await
    }

    /// Selects an option of `input_select` or `select` entities. A bare object id means
    /// `input_select.<id>`.
    pub async fn select_option(&self, target: impl Into<Target>, option: &str) -> Result<()> {
        let (domain, target) = resolve(target, Domain::InputSelect)?;
        let service_data = json!({ "option": option });
        self.call_service(domain, Service::SelectOption, target, service_data).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_comes_from_the_entity_id() {
        assert_eq!(resolve("number.x", Domain::InputNumber).unwrap(), (Domain::Number, Target::entity("number.x")));
        assert_eq!(
            resolve("x", Domain::InputNumber).unwrap(),
            (Domain::InputNumber, Target::entity("input_number.x"))
        );
        assert_eq!(resolve("select.mode", Domain::InputSelect).unwrap().0, Domain::Select);
        // Without entity ids the default domain is used.
        assert_eq!(resolve(Target::area("k"), Domain::InputText).unwrap(), (Domain::InputText, Target::area("k")));
        assert_eq!(resolve(["number.a", "number.b"], Domain::InputNumber).unwrap().0, Domain::Number);
        assert!(matches!(resolve(["number.a", "x"], Domain::InputNumber), Err(Error::InvalidInput(_))));
    }
}
