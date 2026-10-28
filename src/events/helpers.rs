use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer};

pub(crate) mod de_helpers {
    use super::*;

    /// Deserializes an RFC3339 string into a `DateTime<Utc>`.
    pub fn deserialize_rfc3339_string<'de, D>(
        deserializer: D,
    ) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_rfc3339(&s).map_err(serde::de::Error::custom)
    }

    /// Deserializes an optional RFC3339 string into an `Option<DateTime<Utc>>`.
    pub fn deserialize_optional_rfc3339_string<'de, D>(
        deserializer: D,
    ) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<String>::deserialize(deserializer)?
            .map(|s| parse_rfc3339(&s).map_err(serde::de::Error::custom))
            .transpose()
    }

    /// Deserializes a floating-point Unix timestamp into a `DateTime<Utc>`.
    pub fn deserialize_f64_unix_timestamp<'de, D>(
        deserializer: D,
    ) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ts = f64::deserialize(deserializer)?;
        unix_f64_to_datetime(ts).ok_or_else(|| serde::de::Error::custom("Invalid Unix timestamp value"))
    }

    /// Deserializes an optional floating-point Unix timestamp into an `Option<DateTime<Utc>>`.
    pub fn deserialize_optional_f64_unix_timestamp<'de, D>(
        deserializer: D,
    ) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Option::<f64>::deserialize(deserializer)? {
            Some(ts) => unix_f64_to_datetime(ts)
                .map(Some)
                .ok_or_else(|| serde::de::Error::custom("Invalid Unix timestamp value")),
            None => Ok(None),
        }
    }

    fn parse_rfc3339(s: &str) -> Result<DateTime<Utc>, String> {
        DateTime::parse_from_rfc3339(s)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| format!("Invalid timestamp format: {s}, error: {e}"))
    }

    fn unix_f64_to_datetime(ts: f64) -> Option<DateTime<Utc>> {
        if !ts.is_finite() {
            return None;
        }
        DateTime::from_timestamp_micros((ts * 1e6).round() as i64)
    }
}
