use crate::Result;
use serde_json::json;

use crate::{
    state::{Domain, Service},
    HAClient,
};

impl HAClient {
    /// Helper function to simplify setting the value of an `input_number` entity.
    pub async fn set_number(&self, entity_id: &str, value: f64) -> Result<()> {
        let full_entity_id = if entity_id.contains('.') {
            entity_id.to_string()
        } else {
            format!("input_number.{}", entity_id)
        };
        let service_data = json!({ "value": value });
        self.call_service(
            Domain::InputNumber,
            Service::SetValue,
            Some(&full_entity_id),
            Some(service_data),
        )
        .await
    }

    /// Sets the value of an `input_text` entity.
    pub async fn set_text(&self, entity_id: &str, value: &str) -> Result<()> {
        let full_entity_id = if entity_id.contains('.') {
            entity_id.to_string()
        } else {
            format!("input_text.{}", entity_id)
        };
        let service_data = json!({ "value": value });
        self.call_service(
            Domain::InputText,
            Service::SetValue,
            Some(&full_entity_id),
            Some(service_data),
        )
        .await
    }

    /// Selects an option for an `input_select` entity.
    pub async fn select_option(&self, entity_id: &str, option: &str) -> Result<()> {
        let full_entity_id = if entity_id.contains('.') {
            entity_id.to_string()
        } else {
            format!("input_select.{}", entity_id)
        };
        let service_data = json!({ "option": option });
        self.call_service(
            Domain::InputSelect,
            Service::SelectOption,
            Some(&full_entity_id),
            Some(service_data),
        )
        .await
    }
}
