use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use crate::events::helpers::de_helpers::deserialize_optional_rfc3339_string as ts;
use crate::state::Domain;
use crate::state::automation::RunMode;
use crate::state::device_tracker::SourceType;
use crate::state::fan::FanDirection;
use crate::state::light::ColorMode;
use crate::state::media_player::RepeatMode;
use crate::state::number::NumberMode;
use crate::state::climate::{FanMode, HvacAction, HvacMode, PresetMode, SwingMode};
use crate::state::common::UnitOfMeasurement;
use crate::state::device_class::DeviceClass;
use crate::state::state_class::StateClass;

/// Attributes of a `climate` entity. The current HVAC mode is the entity's state.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[non_exhaustive]
pub struct ClimateAttributes {
    pub friendly_name: Option<String>,
    /// Modes the entity supports. Values this library does not know are left out.
    #[serde(default, deserialize_with = "known_values")]
    pub hvac_modes: Option<Vec<HvacMode>>,
    pub hvac_action: Option<HvacAction>,
    pub current_temperature: Option<f64>,
    pub current_humidity: Option<f64>,
    /// Target temperature; `None` in `heat_cool` mode, which uses the range below.
    pub temperature: Option<f64>,
    pub target_temp_low: Option<f64>,
    pub target_temp_high: Option<f64>,
    pub target_temp_step: Option<f64>,
    pub min_temp: Option<f64>,
    pub max_temp: Option<f64>,
    pub fan_mode: Option<FanMode>,
    pub fan_modes: Option<Vec<FanMode>>,
    pub preset_mode: Option<PresetMode>,
    pub preset_modes: Option<Vec<PresetMode>>,
    pub swing_mode: Option<SwingMode>,
    pub swing_modes: Option<Vec<SwingMode>>,
    /// Every other attribute (`supported_features`, integration-specific keys, …).
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}

/// Attributes of any entity without a dedicated type.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[non_exhaustive]
pub struct GenericAttributes {
    pub friendly_name: Option<String>,
    pub unit_of_measurement: Option<UnitOfMeasurement>,
    pub device_class: Option<DeviceClass>,
    pub state_class: Option<StateClass>,
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}

/// Defines a typed attribute struct: the listed fields plus `friendly_name`, `icon` and a
/// flattened `rest` with every other key, and the lookup used by `AttributeSet::get`.
macro_rules! attribute_struct {
    (
        $(#[$meta:meta])*
        pub struct $name:ident {
            $( $(#[$fmeta:meta])* pub $field:ident : $ty:ty ),* $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Default, Deserialize, Serialize)]
        #[non_exhaustive]
        pub struct $name {
            pub friendly_name: Option<String>,
            pub icon: Option<String>,
            $( $(#[$fmeta])* pub $field: $ty, )*
            /// Every other attribute (integration-specific keys, `restored`, …).
            #[serde(flatten)]
            pub rest: Map<String, Value>,
        }

        typed_fields!($name { friendly_name, icon, $($field),* } icon);
    };
}

/// What every variant offers, so `AttributeSet` can dispatch without a match per method.
trait Attrs {
    fn friendly_name(&self) -> Option<&str>;
    fn icon(&self) -> Option<&str>;
    fn rest(&self) -> &Map<String, Value>;
    /// One typed field serialized by its Home Assistant key, without serializing the rest.
    fn typed(&self, key: &str) -> Option<Value>;
}

macro_rules! typed_fields {
    // Structs without an `icon` field find it in `rest`.
    (@icon $s:ident) => { $s.rest.get("icon").and_then(Value::as_str) };
    (@icon $s:ident icon) => { $s.icon.as_deref() };
    ($ty:ty { $($field:ident),* $(,)? } $($icon:ident)?) => {
        impl Attrs for $ty {
            fn friendly_name(&self) -> Option<&str> {
                self.friendly_name.as_deref()
            }
            fn icon(&self) -> Option<&str> {
                typed_fields!(@icon self $($icon)?)
            }
            fn rest(&self) -> &Map<String, Value> {
                &self.rest
            }
            fn typed(&self, key: &str) -> Option<Value> {
                match key {
                    $( stringify!($field) => serde_json::to_value(&self.$field).ok(), )*
                    _ => None,
                }
            }
        }
    };
}

typed_fields!(ClimateAttributes {
    friendly_name, hvac_modes, hvac_action, current_temperature, current_humidity, temperature,
    target_temp_low, target_temp_high, target_temp_step, min_temp, max_temp, fan_mode, fan_modes,
    preset_mode, preset_modes, swing_mode, swing_modes,
});
typed_fields!(GenericAttributes { friendly_name, unit_of_measurement, device_class, state_class });

attribute_struct! {
    /// Attributes of a `sensor` entity.
    pub struct SensorAttributes {
        pub unit_of_measurement: Option<UnitOfMeasurement>,
        pub device_class: Option<DeviceClass>,
        pub state_class: Option<StateClass>,
        /// Allowed states of an `enum` sensor.
        pub options: Option<Vec<String>>,
        #[serde(default, deserialize_with = "ts")]
        pub last_reset: Option<DateTime<Utc>>,
    }
}

attribute_struct! {
    /// Attributes of a `binary_sensor` entity.
    pub struct BinarySensorAttributes {
        pub device_class: Option<DeviceClass>,
    }
}

attribute_struct! {
    /// Attributes of a `light` entity. Color attributes are present only while the light is on.
    /// Shape from Home Assistant's source, not captured from a live instance.
    pub struct LightAttributes {
        pub color_mode: Option<ColorMode>,
        pub supported_color_modes: Option<Vec<ColorMode>>,
        /// 0 to 255.
        pub brightness: Option<u8>,
        pub color_temp_kelvin: Option<u32>,
        pub min_color_temp_kelvin: Option<u32>,
        pub max_color_temp_kelvin: Option<u32>,
        pub hs_color: Option<(f64, f64)>,
        pub xy_color: Option<(f64, f64)>,
        pub rgb_color: Option<(u8, u8, u8)>,
        pub rgbw_color: Option<(u8, u8, u8, u8)>,
        pub rgbww_color: Option<(u8, u8, u8, u8, u8)>,
        pub effect: Option<String>,
        pub effect_list: Option<Vec<String>>,
        pub supported_features: Option<u32>,
    }
}

attribute_struct! {
    /// Attributes of a `cover` entity. Shape from Home Assistant's source, not captured.
    pub struct CoverAttributes {
        /// 0 (closed) to 100 (open).
        pub current_position: Option<u8>,
        pub current_tilt_position: Option<u8>,
        pub device_class: Option<DeviceClass>,
        pub supported_features: Option<u32>,
    }
}

attribute_struct! {
    /// Attributes of a `fan` entity. Shape from Home Assistant's source, not captured.
    pub struct FanAttributes {
        pub percentage: Option<u8>,
        pub percentage_step: Option<f64>,
        pub preset_mode: Option<String>,
        pub preset_modes: Option<Vec<String>>,
        pub oscillating: Option<bool>,
        pub direction: Option<FanDirection>,
        pub supported_features: Option<u32>,
    }
}

attribute_struct! {
    /// Attributes of a `media_player` entity. Shape from Home Assistant's source, not captured.
    pub struct MediaPlayerAttributes {
        /// 0.0 to 1.0.
        pub volume_level: Option<f64>,
        pub is_volume_muted: Option<bool>,
        pub media_content_id: Option<String>,
        pub media_content_type: Option<String>,
        /// Seconds.
        pub media_duration: Option<f64>,
        /// Seconds.
        pub media_position: Option<f64>,
        #[serde(default, deserialize_with = "ts")]
        pub media_position_updated_at: Option<DateTime<Utc>>,
        pub media_title: Option<String>,
        pub media_artist: Option<String>,
        pub media_album_name: Option<String>,
        pub app_name: Option<String>,
        pub source: Option<String>,
        pub sound_mode: Option<String>,
        pub source_list: Option<Vec<String>>,
        pub sound_mode_list: Option<Vec<String>>,
        pub shuffle: Option<bool>,
        pub repeat: Option<RepeatMode>,
        pub entity_picture: Option<String>,
        pub supported_features: Option<u32>,
    }
}

attribute_struct! {
    /// Attributes of a `number` or `input_number` entity.
    pub struct NumberAttributes {
        pub min: Option<f64>,
        pub max: Option<f64>,
        pub step: Option<f64>,
        pub mode: Option<NumberMode>,
        pub unit_of_measurement: Option<UnitOfMeasurement>,
        pub editable: Option<bool>,
    }
}

attribute_struct! {
    /// Attributes of a `select` or `input_select` entity. The current option is the state.
    pub struct SelectAttributes {
        #[serde(default)]
        pub options: Vec<String>,
        pub editable: Option<bool>,
    }
}

attribute_struct! {
    /// Attributes of an `update` entity.
    pub struct UpdateAttributes {
        pub installed_version: Option<String>,
        pub latest_version: Option<String>,
        pub skipped_version: Option<String>,
        pub release_summary: Option<String>,
        pub release_url: Option<String>,
        pub title: Option<String>,
        /// Older Home Assistant sent a percentage here; a non-zero number reads as `true`.
        #[serde(default, deserialize_with = "bool_or_number")]
        pub in_progress: Option<bool>,
        pub update_percentage: Option<f64>,
        pub auto_update: Option<bool>,
        pub display_precision: Option<u32>,
        pub device_class: Option<DeviceClass>,
        pub entity_picture: Option<String>,
        pub supported_features: Option<u32>,
    }
}

/// Wind direction of a `weather` entity: degrees from most integrations, a compass point
/// (`"NNE"`) from some.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum WindBearing {
    Degrees(f64),
    Compass(String),
}

attribute_struct! {
    /// Attributes of a `weather` entity. The condition (`cloudy`, …) is the state.
    pub struct WeatherAttributes {
        pub temperature: Option<f64>,
        pub apparent_temperature: Option<f64>,
        pub dew_point: Option<f64>,
        pub humidity: Option<f64>,
        pub pressure: Option<f64>,
        pub wind_speed: Option<f64>,
        pub wind_gust_speed: Option<f64>,
        pub wind_bearing: Option<WindBearing>,
        pub cloud_coverage: Option<f64>,
        pub uv_index: Option<f64>,
        pub visibility: Option<f64>,
        pub ozone: Option<f64>,
        pub temperature_unit: Option<UnitOfMeasurement>,
        pub pressure_unit: Option<UnitOfMeasurement>,
        pub wind_speed_unit: Option<UnitOfMeasurement>,
        pub visibility_unit: Option<UnitOfMeasurement>,
        pub precipitation_unit: Option<UnitOfMeasurement>,
        pub attribution: Option<String>,
        pub supported_features: Option<u32>,
    }
}

attribute_struct! {
    /// Attributes of the `sun` entity.
    pub struct SunAttributes {
        #[serde(default, deserialize_with = "ts")]
        pub next_dawn: Option<DateTime<Utc>>,
        #[serde(default, deserialize_with = "ts")]
        pub next_dusk: Option<DateTime<Utc>>,
        #[serde(default, deserialize_with = "ts")]
        pub next_midnight: Option<DateTime<Utc>>,
        #[serde(default, deserialize_with = "ts")]
        pub next_noon: Option<DateTime<Utc>>,
        #[serde(default, deserialize_with = "ts")]
        pub next_rising: Option<DateTime<Utc>>,
        #[serde(default, deserialize_with = "ts")]
        pub next_setting: Option<DateTime<Utc>>,
        pub elevation: Option<f64>,
        pub azimuth: Option<f64>,
        pub rising: Option<bool>,
    }
}

attribute_struct! {
    /// Attributes of a `person` entity.
    pub struct PersonAttributes {
        pub latitude: Option<f64>,
        pub longitude: Option<f64>,
        pub gps_accuracy: Option<f64>,
        pub source: Option<String>,
        pub user_id: Option<String>,
        pub id: Option<String>,
        #[serde(default)]
        pub device_trackers: Vec<String>,
        #[serde(default)]
        pub in_zones: Vec<String>,
        pub editable: Option<bool>,
        pub entity_picture: Option<String>,
    }
}

attribute_struct! {
    /// Attributes of a `device_tracker` entity.
    pub struct DeviceTrackerAttributes {
        pub latitude: Option<f64>,
        pub longitude: Option<f64>,
        pub gps_accuracy: Option<f64>,
        pub altitude: Option<f64>,
        pub course: Option<f64>,
        pub speed: Option<f64>,
        pub vertical_accuracy: Option<f64>,
        pub battery_level: Option<f64>,
        pub source_type: Option<SourceType>,
        #[serde(default)]
        pub in_zones: Vec<String>,
    }
}

attribute_struct! {
    /// Attributes of a `zone` entity.
    pub struct ZoneAttributes {
        pub latitude: Option<f64>,
        pub longitude: Option<f64>,
        /// Meters.
        pub radius: Option<f64>,
        pub passive: Option<bool>,
        #[serde(default)]
        pub persons: Vec<String>,
        pub editable: Option<bool>,
    }
}

attribute_struct! {
    /// Attributes of an `automation` entity.
    pub struct AutomationAttributes {
        pub id: Option<String>,
        #[serde(default, deserialize_with = "ts")]
        pub last_triggered: Option<DateTime<Utc>>,
        pub mode: Option<RunMode>,
        /// Runs in progress.
        pub current: Option<u32>,
        /// Most parallel runs allowed.
        pub max: Option<u32>,
    }
}

/// `bool`, or a number (older Home Assistant: update progress) where non-zero means `true`.
fn bool_or_number<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<bool>, D::Error> {
    match Option::<Value>::deserialize(deserializer)? {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(b)),
        Some(Value::Number(n)) => Ok(Some(n.as_f64().is_some_and(|f| f != 0.0))),
        Some(other) => Err(serde::de::Error::custom(format!("expected bool or number, got {other}"))),
    }
}

/// Entity attributes, typed by the entity's domain. `for_entity` picks the variant from the
/// domain (`light.*` is `Light`, `input_number.*` is `Number`, …); entities of other domains,
/// and entities whose attributes do not match their domain's schema, are `Generic`.
/// Deserialized on its own, without an entity id, `Climate` is chosen when climate-specific
/// keys are present and `Generic` otherwise.
///
/// Match with a wildcard arm: variants are added over time.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub enum AttributeSet {
    /// Boxed: much larger than the generic attributes most entities have.
    Climate(Box<ClimateAttributes>),
    Sensor(Box<SensorAttributes>),
    BinarySensor(Box<BinarySensorAttributes>),
    Light(Box<LightAttributes>),
    Cover(Box<CoverAttributes>),
    Fan(Box<FanAttributes>),
    MediaPlayer(Box<MediaPlayerAttributes>),
    /// `number` and `input_number`.
    Number(Box<NumberAttributes>),
    /// `select` and `input_select`.
    Select(Box<SelectAttributes>),
    Update(Box<UpdateAttributes>),
    Weather(Box<WeatherAttributes>),
    Sun(Box<SunAttributes>),
    Person(Box<PersonAttributes>),
    DeviceTracker(Box<DeviceTrackerAttributes>),
    Zone(Box<ZoneAttributes>),
    Automation(Box<AutomationAttributes>),
    Generic(GenericAttributes),
}

macro_rules! accessors {
    ($($method:ident => $variant:ident($ty:ty)),* $(,)?) => {
        $(
            #[doc = concat!("The `", stringify!($variant), "` attributes, if this is that variant.")]
            pub fn $method(&self) -> Option<&$ty> {
                match self {
                    AttributeSet::$variant(a) => Some(a),
                    _ => None,
                }
            }
        )*
    };
}

impl AttributeSet {
    /// Types `value` by the domain of `entity_id`. Never fails.
    pub(crate) fn for_entity(entity_id: &str, value: Value) -> Self {
        use AttributeSet as A;
        let Some(domain) = Domain::of_entity(entity_id) else {
            return Self::deserialize(value).unwrap_or_default();
        };
        match domain.as_str() {
            "climate" => Self::typed(value, A::Climate),
            "sensor" => Self::typed(value, A::Sensor),
            "binary_sensor" => Self::typed(value, A::BinarySensor),
            "light" => Self::typed(value, A::Light),
            "cover" => Self::typed(value, A::Cover),
            "fan" => Self::typed(value, A::Fan),
            "media_player" => Self::typed(value, A::MediaPlayer),
            "number" | "input_number" => Self::typed(value, A::Number),
            "select" | "input_select" => Self::typed(value, A::Select),
            "update" => Self::typed(value, A::Update),
            "weather" => Self::typed(value, A::Weather),
            "sun" => Self::typed(value, A::Sun),
            "person" => Self::typed(value, A::Person),
            "device_tracker" => Self::typed(value, A::DeviceTracker),
            "zone" => Self::typed(value, A::Zone),
            "automation" => Self::typed(value, A::Automation),
            _ => Self::generic(value),
        }
    }

    /// Parses `value` as `T` and wraps it, or falls back to `Generic`.
    fn typed<T: DeserializeOwned>(value: Value, wrap: fn(Box<T>) -> Self) -> Self {
        match T::deserialize(&value) {
            Ok(attrs) => wrap(Box::new(attrs)),
            Err(e) => {
                log::debug!("Attributes did not match the domain's schema ({e}); keeping them untyped.");
                Self::generic(value)
            }
        }
    }

    fn generic(value: Value) -> Self {
        AttributeSet::Generic(lenient_generic(value))
    }

    fn inner(&self) -> &dyn Attrs {
        match self {
            AttributeSet::Climate(a) => a.as_ref(),
            AttributeSet::Sensor(a) => a.as_ref(),
            AttributeSet::BinarySensor(a) => a.as_ref(),
            AttributeSet::Light(a) => a.as_ref(),
            AttributeSet::Cover(a) => a.as_ref(),
            AttributeSet::Fan(a) => a.as_ref(),
            AttributeSet::MediaPlayer(a) => a.as_ref(),
            AttributeSet::Number(a) => a.as_ref(),
            AttributeSet::Select(a) => a.as_ref(),
            AttributeSet::Update(a) => a.as_ref(),
            AttributeSet::Weather(a) => a.as_ref(),
            AttributeSet::Sun(a) => a.as_ref(),
            AttributeSet::Person(a) => a.as_ref(),
            AttributeSet::DeviceTracker(a) => a.as_ref(),
            AttributeSet::Zone(a) => a.as_ref(),
            AttributeSet::Automation(a) => a.as_ref(),
            AttributeSet::Generic(a) => a,
        }
    }

    /// The entity's `friendly_name`, if set.
    pub fn friendly_name(&self) -> Option<&str> {
        self.inner().friendly_name()
    }

    /// The entity's `icon` (such as `mdi:lightbulb`), if set.
    pub fn icon(&self) -> Option<&str> {
        self.inner().icon()
    }

    /// An attribute by its Home Assistant key, as raw JSON, whichever variant holds it.
    pub fn get(&self, key: &str) -> Option<Value> {
        let inner = self.inner();
        inner.rest().get(key).cloned().or_else(|| inner.typed(key)).filter(|v| !v.is_null())
    }

    /// [`get`](Self::get) decoded as `T`; `None` if the attribute is missing or has another type.
    pub fn get_as<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        serde_json::from_value(self.get(key)?).ok()
    }

    accessors! {
        as_climate => Climate(ClimateAttributes),
        as_sensor => Sensor(SensorAttributes),
        as_binary_sensor => BinarySensor(BinarySensorAttributes),
        as_light => Light(LightAttributes),
        as_cover => Cover(CoverAttributes),
        as_fan => Fan(FanAttributes),
        as_media_player => MediaPlayer(MediaPlayerAttributes),
        as_number => Number(NumberAttributes),
        as_select => Select(SelectAttributes),
        as_update => Update(UpdateAttributes),
        as_weather => Weather(WeatherAttributes),
        as_sun => Sun(SunAttributes),
        as_person => Person(PersonAttributes),
        as_device_tracker => DeviceTracker(DeviceTrackerAttributes),
        as_zone => Zone(ZoneAttributes),
        as_automation => Automation(AutomationAttributes),
    }

    /// The generic attributes, if this entity has no dedicated type.
    pub fn as_generic(&self) -> Option<&GenericAttributes> {
        match self {
            AttributeSet::Generic(a) => Some(a),
            _ => None,
        }
    }
}

/// Parses `GenericAttributes`, never failing: if a known key has an unexpected type,
/// the typed fields are left empty and every key is kept in `rest`.
fn lenient_generic(value: Value) -> GenericAttributes {
    match GenericAttributes::deserialize(&value) {
        Ok(attrs) => attrs,
        Err(e) => {
            log::debug!("Attributes did not match the typed schema ({e}); keeping them untyped.");
            let mut attrs = GenericAttributes::default();
            if let Value::Object(map) = value {
                attrs.rest = map;
            }
            attrs
        }
    }
}

/// A list of a closed enum, skipping values it does not know (a newer Home Assistant).
fn known_values<'de, D, T>(deserializer: D) -> Result<Option<Vec<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: for<'a> Deserialize<'a>,
{
    let values = Option::<Vec<Value>>::deserialize(deserializer)?;
    Ok(values.map(|values| values.into_iter().filter_map(|v| T::deserialize(v).ok()).collect()))
}

impl<'de> Deserialize<'de> for AttributeSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;

        if let Value::Object(map) = &value {
            if map.contains_key("hvac_modes") || map.contains_key("current_temperature") {
                if let Ok(climate_attrs) = ClimateAttributes::deserialize(&value) {
                    return Ok(AttributeSet::Climate(Box::new(climate_attrs)));
                }
            }
        }

        Ok(Self::generic(value))
    }
}

impl Default for AttributeSet {
    fn default() -> Self {
        AttributeSet::Generic(GenericAttributes::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unknown_enum_values_do_not_fail() {
        let a: AttributeSet = serde_json::from_value(json!({
            "hvac_modes": ["off", "heat"],
            "fan_modes": ["quiet", "turbo"],
            "current_temperature": 21,
            "friendly_name": "Thermostat"
        }))
        .unwrap();
        let AttributeSet::Climate(c) = &a else { panic!("expected climate: {a:?}") };
        assert_eq!(c.fan_modes.as_ref().unwrap()[0], FanMode::Other("quiet".into()));
        assert_eq!(a.friendly_name(), Some("Thermostat"));

        let a: AttributeSet = serde_json::from_value(json!({"device_class": "wind_direction"})).unwrap();
        let AttributeSet::Generic(g) = a else { panic!() };
        assert_eq!(g.device_class, Some(DeviceClass::Other("wind_direction".into())));
    }

    // Shape captured from a live ESPHome thermostat (name anonymised).
    #[test]
    fn climate_attributes_are_typed() {
        let a = AttributeSet::for_entity("climate.heater", json!({
            "hvac_modes": ["off", "heat", "future_mode"], "min_temp": 10.0, "max_temp": 50.0,
            "target_temp_step": 0.5, "current_temperature": 21.5, "temperature": 23.0,
            "hvac_action": "heating", "friendly_name": "Heater", "supported_features": 385
        }));
        let AttributeSet::Climate(c) = &a else { panic!("expected climate: {a:?}") };
        assert_eq!(c.hvac_modes, Some(vec![HvacMode::Off, HvacMode::Heat]));
        assert_eq!(c.hvac_action, Some(HvacAction::Heating));
        assert_eq!(c.friendly_name.as_deref(), Some("Heater"));
        assert_eq!(c.target_temp_step, Some(0.5));
        assert_eq!(a.get("supported_features"), Some(json!(385)));
        assert_eq!(a.get("temperature"), Some(json!(23.0)));
        assert_eq!(a.get("target_temp_low"), None);
        assert!(!c.rest.contains_key("friendly_name"));
        assert!(a.as_climate().is_some() && a.as_light().is_none() && a.as_generic().is_none());
    }

    #[test]
    fn get_covers_every_typed_field() {
        fn boxed<T: Default>() -> Box<T> {
            Box::default()
        }
        let sets = [
            AttributeSet::Climate(boxed()),
            AttributeSet::Sensor(boxed()),
            AttributeSet::BinarySensor(boxed()),
            AttributeSet::Light(boxed()),
            AttributeSet::Cover(boxed()),
            AttributeSet::Fan(boxed()),
            AttributeSet::MediaPlayer(boxed()),
            AttributeSet::Number(boxed()),
            AttributeSet::Select(boxed()),
            AttributeSet::Update(boxed()),
            AttributeSet::Weather(boxed()),
            AttributeSet::Sun(boxed()),
            AttributeSet::Person(boxed()),
            AttributeSet::DeviceTracker(boxed()),
            AttributeSet::Zone(boxed()),
            AttributeSet::Automation(boxed()),
            AttributeSet::Generic(GenericAttributes::default()),
        ];
        for set in sets {
            let Value::Object(outer) = serde_json::to_value(&set).unwrap() else { panic!() };
            let Some(Value::Object(all)) = outer.into_values().next() else { panic!() };
            assert!(!all.is_empty());
            for key in all.keys() {
                assert!(set.inner().typed(key).is_some(), "typed_fields! misses `{key}` in {set:?}");
            }
        }
    }

    #[test]
    fn malformed_attributes_fall_back_to_untyped() {
        // `current_temperature` as a string breaks the climate schema,
        // `friendly_name` as a number breaks the generic one.
        let a: AttributeSet =
            serde_json::from_value(json!({"current_temperature": "n/a", "friendly_name": 5})).unwrap();
        let AttributeSet::Generic(g) = a else { panic!() };
        assert_eq!(g.friendly_name, None);
        assert_eq!(g.rest["friendly_name"], 5);
        assert_eq!(g.rest["current_temperature"], "n/a");
    }

    /// A payload with a malformed known field falls back to `Generic`, keeping every key.
    fn assert_falls_back(entity: &str, bad: Value) {
        let a = AttributeSet::for_entity(entity, bad.clone());
        let g = a.as_generic().unwrap_or_else(|| panic!("{entity}: expected Generic, got {a:?}"));
        assert_eq!(Value::Object(g.rest.clone()), bad, "{entity}");
    }

    #[test]
    fn malformed_known_field_falls_back_per_domain() {
        for (entity, bad) in [
            ("climate.x", json!({"current_temperature": "x", "a": 1})),
            ("sensor.x", json!({"options": 5, "a": 1})),
            ("binary_sensor.x", json!({"device_class": 5, "a": 1})),
            ("light.x", json!({"brightness": "x", "a": 1})),
            ("cover.x", json!({"current_position": "x", "a": 1})),
            ("fan.x", json!({"percentage": "x", "a": 1})),
            ("media_player.x", json!({"volume_level": "x", "a": 1})),
            ("number.x", json!({"min": "x", "a": 1})),
            ("input_number.x", json!({"max": [], "a": 1})),
            ("select.x", json!({"options": "x", "a": 1})),
            ("input_select.x", json!({"options": 3, "a": 1})),
            ("update.x", json!({"in_progress": "x", "a": 1})),
            ("weather.x", json!({"humidity": "x", "a": 1})),
            ("sun.x", json!({"next_dawn": "yesterday", "a": 1})),
            ("person.x", json!({"latitude": "x", "a": 1})),
            ("device_tracker.x", json!({"speed": "x", "a": 1})),
            ("zone.x", json!({"radius": "x", "a": 1})),
            ("automation.x", json!({"current": "x", "a": 1})),
        ] {
            assert_falls_back(entity, bad);
        }
    }

    #[test]
    fn sensor_attributes() {
        let a = AttributeSet::for_entity("sensor.power", json!({
            "friendly_name": "Power", "icon": "mdi:flash", "unit_of_measurement": "W",
            "device_class": "power", "state_class": "measurement", "options": ["a", "b"],
            "last_reset": "2026-10-29T05:12:00+00:00", "restored": true
        }));
        let s = a.as_sensor().unwrap();
        assert_eq!(s.unit_of_measurement, Some(UnitOfMeasurement::Watts));
        assert_eq!(s.device_class, Some(DeviceClass::Power));
        assert_eq!(s.state_class, Some(StateClass::Measurement));
        assert_eq!(s.options, Some(vec!["a".to_string(), "b".to_string()]));
        assert_eq!(s.last_reset.unwrap().to_rfc3339(), "2026-10-29T05:12:00+00:00");
        assert_eq!(s.rest.len(), 1);
        assert_eq!(a.icon(), Some("mdi:flash"));
        assert_eq!(a.get("restored"), Some(json!(true)));
        assert_eq!(a.get_as::<String>("unit_of_measurement"), Some("W".into()));
        assert_eq!(a.get_as::<u32>("unit_of_measurement"), None);
        // `null` timestamps are allowed.
        let a = AttributeSet::for_entity("sensor.x", json!({"last_reset": null}));
        assert!(a.as_sensor().unwrap().last_reset.is_none());
    }

    #[test]
    fn binary_sensor_attributes() {
        let a = AttributeSet::for_entity("binary_sensor.door", json!({
            "friendly_name": "Door", "icon": "mdi:door", "device_class": "door", "extra": 1
        }));
        let b = a.as_binary_sensor().unwrap();
        assert_eq!(b.device_class, Some(DeviceClass::Door));
        assert_eq!(b.rest.len(), 1);
        assert_eq!(a.icon(), Some("mdi:door"));
    }

    // Shape from Home Assistant's source, not captured from a live instance.
    #[test]
    fn light_attributes() {
        let a = AttributeSet::for_entity("light.lamp", json!({
            "friendly_name": "Lamp", "icon": "mdi:lamp", "color_mode": "rgbw",
            "supported_color_modes": ["color_temp", "hs", "rgbw", "future"],
            "brightness": 200, "color_temp_kelvin": 3000, "min_color_temp_kelvin": 2000,
            "max_color_temp_kelvin": 6500, "hs_color": [30.5, 60], "xy_color": [0.4, 0.5],
            "rgb_color": [255, 128, 0], "rgbw_color": [255, 128, 0, 10],
            "rgbww_color": [255, 128, 0, 10, 20], "effect": "rainbow", "effect_list": ["rainbow"],
            "supported_features": 4, "extra": "x"
        }));
        let l = a.as_light().unwrap();
        assert_eq!(l.color_mode, Some(ColorMode::Rgbw));
        assert_eq!(l.supported_color_modes.as_ref().unwrap()[3], ColorMode::Other("future".into()));
        assert_eq!(l.brightness, Some(200));
        assert_eq!(l.color_temp_kelvin, Some(3000));
        assert_eq!((l.min_color_temp_kelvin, l.max_color_temp_kelvin), (Some(2000), Some(6500)));
        assert_eq!(l.hs_color, Some((30.5, 60.0)));
        assert_eq!(l.xy_color, Some((0.4, 0.5)));
        assert_eq!(l.rgb_color, Some((255, 128, 0)));
        assert_eq!(l.rgbw_color, Some((255, 128, 0, 10)));
        assert_eq!(l.rgbww_color, Some((255, 128, 0, 10, 20)));
        assert_eq!(l.effect.as_deref(), Some("rainbow"));
        assert_eq!(l.effect_list, Some(vec!["rainbow".to_string()]));
        assert_eq!(l.supported_features, Some(4));
        assert_eq!(l.rest.len(), 1);
        assert_eq!(a.get("brightness"), Some(json!(200)));
        assert_eq!(a.icon(), Some("mdi:lamp"));
        // An off light has no color attributes.
        let off = AttributeSet::for_entity("light.lamp", json!({"supported_color_modes": ["onoff"], "color_mode": null}));
        assert_eq!(off.as_light().unwrap().brightness, None);
    }

    // Shape from Home Assistant's source, not captured from a live instance.
    #[test]
    fn cover_attributes() {
        let a = AttributeSet::for_entity("cover.blind", json!({
            "friendly_name": "Blind", "icon": "mdi:blinds", "current_position": 40,
            "current_tilt_position": 10, "device_class": "blind", "supported_features": 15, "extra": 1
        }));
        let c = a.as_cover().unwrap();
        assert_eq!((c.current_position, c.current_tilt_position), (Some(40), Some(10)));
        assert_eq!(c.device_class, Some(DeviceClass::Other("blind".into())));
        assert_eq!(c.supported_features, Some(15));
        assert_eq!(c.rest.len(), 1);
    }

    // Shape from Home Assistant's source, not captured from a live instance.
    #[test]
    fn fan_attributes() {
        let a = AttributeSet::for_entity("fan.vent", json!({
            "friendly_name": "Vent", "icon": "mdi:fan", "percentage": 50, "percentage_step": 33.3,
            "preset_mode": "auto", "preset_modes": ["auto", "sleep"], "oscillating": true,
            "direction": "reverse", "supported_features": 57, "extra": 1
        }));
        let f = a.as_fan().unwrap();
        assert_eq!(f.percentage, Some(50));
        assert_eq!(f.percentage_step, Some(33.3));
        assert_eq!(f.preset_mode.as_deref(), Some("auto"));
        assert_eq!(f.preset_modes.as_ref().unwrap().len(), 2);
        assert_eq!(f.oscillating, Some(true));
        assert_eq!(f.direction, Some(FanDirection::Reverse));
        assert_eq!(f.supported_features, Some(57));
        assert_eq!(f.rest.len(), 1);
    }

    // Shape from Home Assistant's source, not captured from a live instance.
    #[test]
    fn media_player_attributes() {
        let a = AttributeSet::for_entity("media_player.tv", json!({
            "friendly_name": "TV", "icon": "mdi:tv", "volume_level": 0.25, "is_volume_muted": false,
            "media_content_id": "id1", "media_content_type": "music", "media_duration": 215,
            "media_position": 12, "media_position_updated_at": "2026-10-29T05:12:00+00:00",
            "media_title": "Song", "media_artist": "Band", "media_album_name": "Album",
            "app_name": "Player", "source": "HDMI 1", "sound_mode": "movie",
            "source_list": ["HDMI 1", "HDMI 2"], "sound_mode_list": ["movie", "music"],
            "shuffle": true, "repeat": "all", "entity_picture": "/api/x", "supported_features": 152511,
            "extra": 1
        }));
        let m = a.as_media_player().unwrap();
        assert_eq!(m.volume_level, Some(0.25));
        assert_eq!(m.is_volume_muted, Some(false));
        assert_eq!(m.media_content_id.as_deref(), Some("id1"));
        assert_eq!(m.media_content_type.as_deref(), Some("music"));
        assert_eq!((m.media_duration, m.media_position), (Some(215.0), Some(12.0)));
        assert!(m.media_position_updated_at.is_some());
        assert_eq!(m.media_title.as_deref(), Some("Song"));
        assert_eq!(m.media_artist.as_deref(), Some("Band"));
        assert_eq!(m.media_album_name.as_deref(), Some("Album"));
        assert_eq!(m.app_name.as_deref(), Some("Player"));
        assert_eq!(m.source.as_deref(), Some("HDMI 1"));
        assert_eq!(m.sound_mode.as_deref(), Some("movie"));
        assert_eq!(m.source_list.as_ref().unwrap().len(), 2);
        assert_eq!(m.sound_mode_list.as_ref().unwrap().len(), 2);
        assert_eq!(m.shuffle, Some(true));
        assert_eq!(m.repeat, Some(RepeatMode::All));
        assert_eq!(m.entity_picture.as_deref(), Some("/api/x"));
        assert_eq!(m.supported_features, Some(152511));
        assert_eq!(m.rest.len(), 1);
    }

    #[test]
    fn number_attributes() {
        for entity in ["number.x", "input_number.x"] {
            let a = AttributeSet::for_entity(entity, json!({
                "friendly_name": "N", "icon": "mdi:gauge", "min": 0, "max": 100.0, "step": 0.5,
                "mode": "slider", "unit_of_measurement": "°C", "editable": true, "extra": 1
            }));
            let n = a.as_number().unwrap();
            assert_eq!((n.min, n.max, n.step), (Some(0.0), Some(100.0), Some(0.5)));
            assert_eq!(n.mode, Some(NumberMode::Slider));
            assert_eq!(n.unit_of_measurement, Some(UnitOfMeasurement::Celsius));
            assert_eq!(n.editable, Some(true));
            assert_eq!(n.rest.len(), 1);
        }
    }

    #[test]
    fn select_attributes() {
        for entity in ["select.x", "input_select.x"] {
            let a = AttributeSet::for_entity(entity, json!({
                "friendly_name": "S", "icon": "mdi:menu", "options": ["off", "on"], "editable": false, "extra": 1
            }));
            let s = a.as_select().unwrap();
            assert_eq!(s.options, vec!["off", "on"]);
            assert_eq!(s.editable, Some(false));
            assert_eq!(s.rest.len(), 1);
        }
        let a = AttributeSet::for_entity("select.x", json!({}));
        assert!(a.as_select().unwrap().options.is_empty());
    }

    #[test]
    fn update_attributes() {
        let a = AttributeSet::for_entity("update.firmware", json!({
            "friendly_name": "Firmware", "icon": "mdi:update", "installed_version": "1.0",
            "latest_version": "1.1", "skipped_version": null, "release_summary": "Fixes",
            "release_url": "https://example.com/r", "title": "Firmware", "in_progress": false,
            "update_percentage": 12.5, "auto_update": true, "display_precision": 0,
            "device_class": "firmware", "entity_picture": "/x", "supported_features": 23, "extra": 1
        }));
        let u = a.as_update().unwrap();
        assert_eq!(u.installed_version.as_deref(), Some("1.0"));
        assert_eq!(u.latest_version.as_deref(), Some("1.1"));
        assert_eq!(u.skipped_version, None);
        assert_eq!(u.release_summary.as_deref(), Some("Fixes"));
        assert_eq!(u.release_url.as_deref(), Some("https://example.com/r"));
        assert_eq!(u.title.as_deref(), Some("Firmware"));
        assert_eq!(u.in_progress, Some(false));
        assert_eq!(u.update_percentage, Some(12.5));
        assert_eq!(u.auto_update, Some(true));
        assert_eq!(u.display_precision, Some(0));
        assert_eq!(u.device_class, Some(DeviceClass::Other("firmware".into())));
        assert_eq!(u.entity_picture.as_deref(), Some("/x"));
        assert_eq!(u.supported_features, Some(23));
        assert_eq!(u.rest.len(), 1);

        let old = AttributeSet::for_entity("update.firmware", json!({"in_progress": 45}));
        assert_eq!(old.as_update().unwrap().in_progress, Some(true));
        let none = AttributeSet::for_entity("update.firmware", json!({}));
        assert_eq!(none.as_update().unwrap().in_progress, None);
    }

    #[test]
    fn weather_attributes_and_ints_decode_into_f64() {
        let a = AttributeSet::for_entity("weather.home", json!({
            "friendly_name": "Home", "icon": "mdi:cloud", "temperature": 12.5, "apparent_temperature": 11,
            "dew_point": 8.1, "humidity": 64, "pressure": 1013.2, "wind_speed": 5.5,
            "wind_gust_speed": 9, "wind_bearing": 180, "cloud_coverage": 80.0, "uv_index": 1.5,
            "visibility": 10, "ozone": 30, "temperature_unit": "°C", "pressure_unit": "hPa",
            "wind_speed_unit": "km/h", "visibility_unit": "km", "precipitation_unit": "mm",
            "attribution": "Provider", "supported_features": 7, "extra": 1
        }));
        let w = a.as_weather().unwrap();
        assert_eq!(w.humidity, Some(64.0));
        assert_eq!(w.apparent_temperature, Some(11.0));
        assert_eq!(w.temperature, Some(12.5));
        assert_eq!(w.dew_point, Some(8.1));
        assert_eq!(w.pressure, Some(1013.2));
        assert_eq!((w.wind_speed, w.wind_gust_speed), (Some(5.5), Some(9.0)));
        assert_eq!(w.wind_bearing, Some(WindBearing::Degrees(180.0)));
        let compass = AttributeSet::for_entity("weather.x", json!({"wind_bearing": "NNE"}));
        assert_eq!(compass.as_weather().unwrap().wind_bearing, Some(WindBearing::Compass("NNE".into())));
        assert_eq!((w.cloud_coverage, w.uv_index, w.visibility, w.ozone), (Some(80.0), Some(1.5), Some(10.0), Some(30.0)));
        assert_eq!(w.temperature_unit, Some(UnitOfMeasurement::Celsius));
        assert_eq!(w.pressure_unit, Some(UnitOfMeasurement::Other("hPa".into())));
        assert_eq!(w.wind_speed_unit.as_ref().unwrap().as_str(), "km/h");
        assert_eq!(w.visibility_unit.as_ref().unwrap().as_str(), "km");
        assert_eq!(w.precipitation_unit.as_ref().unwrap().as_str(), "mm");
        assert_eq!(w.attribution.as_deref(), Some("Provider"));
        assert_eq!(w.supported_features, Some(7));
        assert_eq!(w.rest.len(), 1);
    }

    #[test]
    fn sun_attributes() {
        let a = AttributeSet::for_entity("sun.sun", json!({
            "friendly_name": "Sun", "next_dawn": "2026-10-29T04:00:00+00:00",
            "next_dusk": "2026-10-29T16:00:00+00:00", "next_midnight": "2026-10-29T22:00:00+00:00",
            "next_noon": "2026-10-29T10:00:00+00:00", "next_rising": "2026-10-29T04:30:00+00:00",
            "next_setting": "2026-10-29T15:30:00+00:00", "elevation": -12, "azimuth": 301.2,
            "rising": true, "extra": 1
        }));
        let s = a.as_sun().unwrap();
        assert!(s.next_dawn.is_some() && s.next_dusk.is_some() && s.next_midnight.is_some());
        assert!(s.next_noon.is_some() && s.next_rising.is_some() && s.next_setting.is_some());
        assert_eq!((s.elevation, s.azimuth, s.rising), (Some(-12.0), Some(301.2), Some(true)));
        assert_eq!(s.rest.len(), 1);
    }

    #[test]
    fn person_attributes() {
        let a = AttributeSet::for_entity("person.alex", json!({
            "friendly_name": "Alex", "icon": "mdi:account", "latitude": 50.1, "longitude": 30.2,
            "gps_accuracy": 15, "source": "device_tracker.phone", "user_id": "u1", "id": "p1",
            "device_trackers": ["device_tracker.phone"], "in_zones": ["zone.home"], "editable": true,
            "entity_picture": "/p", "extra": 1
        }));
        let p = a.as_person().unwrap();
        assert_eq!((p.latitude, p.longitude, p.gps_accuracy), (Some(50.1), Some(30.2), Some(15.0)));
        assert_eq!(p.source.as_deref(), Some("device_tracker.phone"));
        assert_eq!((p.user_id.as_deref(), p.id.as_deref()), (Some("u1"), Some("p1")));
        assert_eq!(p.device_trackers, vec!["device_tracker.phone"]);
        assert_eq!(p.in_zones, vec!["zone.home"]);
        assert_eq!(p.editable, Some(true));
        assert_eq!(p.entity_picture.as_deref(), Some("/p"));
        assert_eq!(p.rest.len(), 1);
        let empty = AttributeSet::for_entity("person.alex", json!({}));
        assert!(empty.as_person().unwrap().device_trackers.is_empty());
    }

    #[test]
    fn device_tracker_attributes() {
        let a = AttributeSet::for_entity("device_tracker.phone", json!({
            "friendly_name": "Phone", "latitude": 50.1, "longitude": 30.2, "gps_accuracy": 10.5,
            "altitude": 120.0, "course": 90, "speed": 3, "vertical_accuracy": 4, "battery_level": 77,
            "source_type": "gps", "in_zones": ["zone.home"], "extra": 1
        }));
        let d = a.as_device_tracker().unwrap();
        assert_eq!((d.latitude, d.longitude, d.gps_accuracy), (Some(50.1), Some(30.2), Some(10.5)));
        assert_eq!((d.altitude, d.course, d.speed), (Some(120.0), Some(90.0), Some(3.0)));
        assert_eq!((d.vertical_accuracy, d.battery_level), (Some(4.0), Some(77.0)));
        assert_eq!(d.source_type, Some(SourceType::Gps));
        assert_eq!(d.in_zones, vec!["zone.home"]);
        assert_eq!(d.rest.len(), 1);
    }

    #[test]
    fn zone_attributes() {
        let a = AttributeSet::for_entity("zone.home", json!({
            "friendly_name": "Home", "icon": "mdi:home", "latitude": 50.1, "longitude": 30.2,
            "radius": 100, "passive": false, "persons": ["person.alex"], "editable": true, "extra": 1
        }));
        let z = a.as_zone().unwrap();
        assert_eq!((z.latitude, z.longitude, z.radius), (Some(50.1), Some(30.2), Some(100.0)));
        assert_eq!(z.passive, Some(false));
        assert_eq!(z.persons, vec!["person.alex"]);
        assert_eq!(z.editable, Some(true));
        assert_eq!(z.rest.len(), 1);
    }

    #[test]
    fn automation_attributes() {
        let a = AttributeSet::for_entity("automation.lights", json!({
            "friendly_name": "Lights", "id": "1700000000000", "last_triggered": "2026-10-29T05:12:00+00:00",
            "mode": "restart", "current": 1, "max": 10, "supported_features": 0, "extra": 1
        }));
        let m = a.as_automation().unwrap();
        assert_eq!(m.id.as_deref(), Some("1700000000000"));
        assert!(m.last_triggered.is_some());
        assert_eq!(m.mode, Some(RunMode::Restart));
        assert_eq!((m.current, m.max), (Some(1), Some(10)));
        assert_eq!(m.rest.len(), 2);
        let never = AttributeSet::for_entity("automation.lights", json!({"last_triggered": null, "mode": "single"}));
        assert!(never.as_automation().unwrap().last_triggered.is_none());
    }

    #[test]
    fn generic_and_domainless() {
        let a = AttributeSet::for_entity("switch.x", json!({"icon": "mdi:power", "device_class": "switch"}));
        assert_eq!(a.as_generic().unwrap().device_class, Some(DeviceClass::Other("switch".into())));
        assert_eq!(a.icon(), Some("mdi:power"));
        let b: AttributeSet = serde_json::from_value(json!({"brightness": 3})).unwrap();
        assert!(b.as_generic().is_some());
    }
}
