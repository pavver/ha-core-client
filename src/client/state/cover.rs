use std::fmt;

/// Represents the state of a Cover entity.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum CoverState {
    Open,
    Opening,
    Closed,
    Closing,
}

impl CoverState {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "open" => Some(CoverState::Open),
            "opening" => Some(CoverState::Opening),
            "closed" => Some(CoverState::Closed),
            "closing" => Some(CoverState::Closing),
            _ => None,
        }
    }

    pub fn to_str(&self) -> &'static str {
        match self {
            CoverState::Open => "open",
            CoverState::Opening => "opening",
            CoverState::Closed => "closed",
            CoverState::Closing => "closing",
        }
    }
}

impl fmt::Display for CoverState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_str())
    }
}
