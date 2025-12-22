
    async fn process_state_change(&mut self, entity_id: &str, state: &str) -> Result<()> {
        // Приклад логіки: якщо напруга нижче 12V, увімкнути зарядку
        if entity_id == "sensor.battery_1_voltage" {
            if let Ok(voltage) = state.parse::<f64>() {
                if voltage < 12.0 {
                    info!("Low battery! Turning on charger...");
                    self.call_service("switch", "turn_on", "switch.charger")
                        .await?;
                }
            }
        }

        Ok(())
    }

    pub async fn set_helper_state(&mut self, entity_id: &str, value: f64) -> Result<()> {
        let set_state_msg = json!({
            "id": self.msg_id,
            "type": "call_service",
            "domain": "input_number",
            "service": "set_value",
            "service_data": {
                "entity_id": entity_id,
                "value": value
            }
        });

        self.ws
            .send(Message::Text(set_state_msg.to_string().into()))
            .await?;
        info!("Set {} to {}", entity_id, value);
        self.msg_id += 1;

        Ok(())
    }

    
    pub async fn call_service(
        &mut self,
        domain: &str,
        service: &str,
        entity_id: &str,
    ) -> Result<()> {
        let service_msg = json!({
            "id": self.msg_id,
            "type": "call_service",
            "domain": domain,
            "service": service,
            "service_data": {
                "entity_id": entity_id
            }
        });

        self.ws
            .send(Message::Text(service_msg.to_string().into()))
            .await?;
        info!("Called service {}.{} for {}", domain, service, entity_id);
        self.msg_id += 1;

        Ok(())
    }