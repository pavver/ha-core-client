use strum::{EnumString, Display};
use serde::Serialize;

/// Represents the state of a Lock entity.
#[derive(Debug, PartialEq, Clone, Copy, EnumString, Display, Serialize)]
#[strum(serialize_all = "snake_case")]
pub enum LockState {
    Locked,
    Unlocked,
    Locking,
    Unlocking,
    Jammed,
}
