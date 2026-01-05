use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumString};

#[derive(Debug, PartialEq, Clone, Copy, Deserialize, EnumString, Display, AsRefStr, Serialize)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case", ascii_case_insensitive)]
pub enum StateClass {
    Measurement,
    Total,
    TotalIncreasing,
}
