use std::fmt;

/// Represents the state of a Lock entity.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum LockState {
    Locked,
    Unlocked,
    Locking,
    Unlocking,
    Jammed,
}

impl LockState {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "locked" => Some(LockState::Locked),
            "unlocked" => Some(LockState::Unlocked),
            "locking" => Some(LockState::Locking),
            "unlocking" => Some(LockState::Unlocking),
            "jammed" => Some(LockState::Jammed),
            _ => None,
        }
    }

    pub fn to_str(&self) -> &'static str {
        match self {
            LockState::Locked => "locked",
            LockState::Unlocked => "unlocked",
            LockState::Locking => "locking",
            LockState::Unlocking => "unlocking",
            LockState::Jammed => "jammed",
        }
    }
}

impl fmt::Display for LockState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_str())
    }
}
