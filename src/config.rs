use serde_json::Value;
use std::fs;

pub struct Config {
    pub url: String,
    pub token: String,
}

fn config_read_key<'a>(config: &'a Value, key: &'a str) -> &'a str {
    match &config[key] {
        Value::String(s) => return s.as_str(),
        _default => panic!("Failed to read '{}' in the config file", key),
    }
}

pub fn read_config() -> Config {
    let file_path = "config.json";
    let file_content = fs::read_to_string(file_path).expect("Failed to read the config file");
    let config: Value =
        serde_json::from_str(&file_content).expect("Failed to parse the config file");

    let url = config_read_key(&config, "HA_URL");
    let token = config_read_key(&config, "HA_TOKEN");

    Config {
        url: url.to_string(),
        token: token.to_string(),
    }
}
