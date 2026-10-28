use serde::Serialize;

/// What a service call acts on. Serializes to Home Assistant's `target` object; empty lists are omitted.
///
/// Anything that converts into a `Target` can be passed to
/// [`HAClient::call_service`](crate::HAClient::call_service): `()` (no target), a single
/// entity id (`"light.kitchen"`), or a list of entity ids. Use the constructors for other kinds.
///
/// ```
/// use ha_core_client::Target;
/// let target = Target::area("kitchen").and_entity("light.porch");
/// assert!(!target.is_empty());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct Target {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub entity_id: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub device_id: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub area_id: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub floor_id: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub label_id: Vec<String>,
}

impl Target {
    /// An empty target (the service acts on its own data only).
    pub fn new() -> Self {
        Self::default()
    }

    /// Targets one entity.
    pub fn entity(id: impl Into<String>) -> Self {
        Self::new().and_entity(id)
    }

    /// Targets several entities.
    pub fn entities<I, S>(ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self { entity_id: ids.into_iter().map(Into::into).collect(), ..Self::default() }
    }

    /// Targets one device.
    pub fn device(id: impl Into<String>) -> Self {
        Self::new().and_device(id)
    }

    /// Targets one area.
    pub fn area(id: impl Into<String>) -> Self {
        Self::new().and_area(id)
    }

    /// Targets one floor.
    pub fn floor(id: impl Into<String>) -> Self {
        Self::new().and_floor(id)
    }

    /// Targets one label.
    pub fn label(id: impl Into<String>) -> Self {
        Self::new().and_label(id)
    }

    /// Adds an entity.
    pub fn and_entity(mut self, id: impl Into<String>) -> Self {
        self.entity_id.push(id.into());
        self
    }

    /// Adds a device.
    pub fn and_device(mut self, id: impl Into<String>) -> Self {
        self.device_id.push(id.into());
        self
    }

    /// Adds an area.
    pub fn and_area(mut self, id: impl Into<String>) -> Self {
        self.area_id.push(id.into());
        self
    }

    /// Adds a floor.
    pub fn and_floor(mut self, id: impl Into<String>) -> Self {
        self.floor_id.push(id.into());
        self
    }

    /// Adds a label.
    pub fn and_label(mut self, id: impl Into<String>) -> Self {
        self.label_id.push(id.into());
        self
    }

    /// `true` if nothing is targeted.
    pub fn is_empty(&self) -> bool {
        self.entity_id.is_empty()
            && self.device_id.is_empty()
            && self.area_id.is_empty()
            && self.floor_id.is_empty()
            && self.label_id.is_empty()
    }
}

impl From<()> for Target {
    fn from(_: ()) -> Self {
        Self::new()
    }
}

impl From<&str> for Target {
    fn from(id: &str) -> Self {
        Self::entity(id)
    }
}

impl From<String> for Target {
    fn from(id: String) -> Self {
        Self::entity(id)
    }
}

impl From<&String> for Target {
    fn from(id: &String) -> Self {
        Self::entity(id.as_str())
    }
}

impl<const N: usize> From<[&str; N]> for Target {
    fn from(ids: [&str; N]) -> Self {
        Self::entities(ids)
    }
}

impl From<Vec<String>> for Target {
    fn from(ids: Vec<String>) -> Self {
        Self::entities(ids)
    }
}

impl From<Vec<&str>> for Target {
    fn from(ids: Vec<&str>) -> Self {
        Self::entities(ids)
    }
}

impl From<&[&str]> for Target {
    fn from(ids: &[&str]) -> Self {
        Self::entities(ids.iter().copied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, to_value};

    #[test]
    fn empty_serializes_to_empty_object() {
        assert_eq!(to_value(Target::new()).unwrap(), json!({}));
        assert!(Target::new().is_empty());
    }

    #[test]
    fn constructors() {
        assert_eq!(to_value(Target::entity("light.a")).unwrap(), json!({"entity_id": ["light.a"]}));
        assert_eq!(to_value(Target::entities(["a.x", "a.y"])).unwrap(), json!({"entity_id": ["a.x", "a.y"]}));
        assert_eq!(to_value(Target::device("d1")).unwrap(), json!({"device_id": ["d1"]}));
        assert_eq!(to_value(Target::area("kitchen")).unwrap(), json!({"area_id": ["kitchen"]}));
        assert_eq!(to_value(Target::floor("f")).unwrap(), json!({"floor_id": ["f"]}));
        assert_eq!(to_value(Target::label("l")).unwrap(), json!({"label_id": ["l"]}));
    }

    #[test]
    fn builders_combine() {
        let t = Target::area("kitchen").and_entity("light.a").and_entity("light.b").and_device("d").and_floor("f").and_label("l");
        assert!(!t.is_empty());
        assert_eq!(
            to_value(t).unwrap(),
            json!({"entity_id": ["light.a", "light.b"], "device_id": ["d"], "area_id": ["kitchen"],
                   "floor_id": ["f"], "label_id": ["l"]})
        );
        assert_eq!(Target::entity("a.b").and_area("x"), Target::area("x").and_entity("a.b"));
    }

    #[test]
    fn conversions_are_entity_ids() {
        let one = Target::entity("light.a");
        let s = String::from("light.a");
        assert_eq!(Target::from("light.a"), one);
        assert_eq!(Target::from(s.clone()), one);
        assert_eq!(Target::from(&s), one);
        assert_eq!(Target::from(()), Target::new());
        let two = Target::entities(["light.a", "light.b"]);
        assert_eq!(Target::from(["light.a", "light.b"]), two);
        assert_eq!(Target::from(vec!["light.a", "light.b"]), two);
        assert_eq!(Target::from(vec!["light.a".to_string(), "light.b".to_string()]), two);
        let slice: &[&str] = &["light.a", "light.b"];
        assert_eq!(Target::from(slice), two);
    }
}
