use chrono::{DateTime, TimeZone, Utc};
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
        DateTime::parse_from_rfc3339(&s)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| {
                serde::de::Error::custom(format!("Invalid timestamp format: {}, error: {}", s, e))
            })
    }

    /// Deserializes a floating-point Unix timestamp into a `DateTime<Utc>`.
    pub fn deserialize_f64_unix_timestamp<'de, D>(
        deserializer: D,
    ) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ts_f64 = f64::deserialize(deserializer)?;
        let secs = ts_f64 as i64;
        let nanos = ((ts_f64 - secs as f64) * 1e9) as u32;
        Utc.timestamp_opt(secs, nanos)
            .single()
            .ok_or_else(|| serde::de::Error::custom("Invalid Unix timestamp value"))
    }

    /// Deserializes an optional floating-point Unix timestamp into an `Option<DateTime<Utc>>`.
    pub fn deserialize_optional_f64_unix_timestamp<'de, D>(
        deserializer: D,
    ) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt = Option::<f64>::deserialize(deserializer)?;
        match opt {
            Some(ts_f64) => {
                let secs = ts_f64 as i64;
                let nanos = ((ts_f64 - secs as f64) * 1e9) as u32;
                Utc.timestamp_opt(secs, nanos)
                    .single()
                    .map(Some)
                    .ok_or_else(|| serde::de::Error::custom("Invalid Unix timestamp value"))
            }
            None => Ok(None),
        }
    }
}
