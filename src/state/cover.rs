use strum::{EnumString, Display};
use serde::Serialize;

/// Represents the state of a Cover entity.
#[derive(Debug, PartialEq, Clone, Copy, EnumString, Display, Serialize)]
#[strum(serialize_all = "snake_case")]
pub enum CoverState {
    Open,
    Opening,
    Closed,
    Closing,
}
