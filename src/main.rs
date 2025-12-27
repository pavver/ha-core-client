use client::HAClient;
use log::info;
use tracing_subscriber::EnvFilter;

pub mod client;
mod config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // --- Setup ---
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let cfg = config::read_config();
    let mut client = HAClient::new();

    // --- Main Logic ---
    client.run(&cfg.url, &cfg.token).await?;
    info!("Client is running.");

    // Let the client run for a while
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    }
}
