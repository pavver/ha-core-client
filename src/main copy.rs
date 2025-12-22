use std::sync::Arc;
use tracing::info;

mod client;
mod config;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let config = config::read_config();

    let mut client = client::HAClient::new(&config.url, &config.token)
        .await
        .expect("Failed to connect");

    // Підписка на сутність
    let subscription_callback = Arc::new(|event: &serde_json::Value| {
        println!("Received event: {:?}\n\n", event);
    });

    client
        .subscribe_entity("sensor.battery_1_voltage", subscription_callback)
        .await
        .expect("Failed to subscribe");

    // Запуск обробки подій
    client.run().await.expect("Failed to run client");
}
