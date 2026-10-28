use strum::{EnumString, Display};
use serde::{Deserialize, Serialize};

/// Represents the state of a Lock entity.
#[derive(Debug, PartialEq, Clone, Copy, EnumString, Display, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum LockState {
    Locked,
    Unlocked,
    Locking,
    Unlocking,
    Jammed,
    Open,
    Opening,
}
