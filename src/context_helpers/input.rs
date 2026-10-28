use crate::Result;
use serde_json::json;

use crate::{
    state::{Domain, Service},
    HAClient,
};

/// Resolves `entity_id` and the domain whose service to call: a bare object id
/// (`"target"`) gets `default` as its domain, a full id keeps its own (`number.x`).
fn resolve(entity_id: &str, default: Domain) -> (Domain, String) {
    match Domain::of_entity(entity_id) {
        Some(domain) => (domain, entity_id.to_string()),
        None => {
            let full = format!("{default}.{entity_id}");
            (default, full)
        }
    }
}

impl HAClient {
    /// Sets the value of an `input_number` or `number` entity. A bare object id
    /// (`"target_temp"`) means `input_number.target_temp`.
    pub async fn set_number(&self, entity_id: &str, value: f64) -> Result<()> {
        let (domain, entity_id) = resolve(entity_id, Domain::InputNumber);
        let service_data = json!({ "value": value });
        self.call_service(domain, Service::SetValue, entity_id, service_data).await
    }

    /// Sets the value of an `input_text` or `text` entity. A bare object id means `input_text.<id>`.
    pub async fn set_text(&self, entity_id: &str, value: &str) -> Result<()> {
        let (domain, entity_id) = resolve(entity_id, Domain::InputText);
        let service_data = json!({ "value": value });
        self.call_service(domain, Service::SetValue, entity_id, service_data).await
    }

    /// Selects an option of an `input_select` or `select` entity. A bare object id means
    /// `input_select.<id>`.
    pub async fn select_option(&self, entity_id: &str, option: &str) -> Result<()> {
        let (domain, entity_id) = resolve(entity_id, Domain::InputSelect);
        let service_data = json!({ "option": option });
        self.call_service(domain, Service::SelectOption, entity_id, service_data).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_comes_from_the_entity_id() {
        assert_eq!(resolve("number.x", Domain::InputNumber), (Domain::Number, "number.x".into()));
        assert_eq!(resolve("x", Domain::InputNumber), (Domain::InputNumber, "input_number.x".into()));
        assert_eq!(resolve("select.mode", Domain::InputSelect), (Domain::Select, "select.mode".into()));
    }
}
