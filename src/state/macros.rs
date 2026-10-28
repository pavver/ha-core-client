/// Defines a string-backed enum with known variants and an `Other(String)` fallback.
///
/// Generates `Display`, `FromStr` (infallible), `From<&str>`, `From<String>`,
/// `as_str()`, `Serialize` and `Deserialize`. Unknown strings round-trip through
/// `Other`, so values Home Assistant adds later never break deserialization.
macro_rules! string_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $( $(#[$vmeta:meta])* $variant:ident => $s:literal $(| $alias:literal)* ),* $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum $name {
            $( $(#[$vmeta])* $variant, )*
            /// A value not known to this library, kept verbatim.
            Other(String),
        }

        impl $name {
            /// The string Home Assistant uses for this value.
            pub fn as_str(&self) -> &str {
                match self {
                    $( Self::$variant => $s, )*
                    Self::Other(s) => s.as_str(),
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                match s {
                    $( $s $(| $alias)* => Self::$variant, )*
                    other => Self::Other(other.to_string()),
                }
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                match Self::from(s.as_str()) {
                    Self::Other(_) => Self::Other(s),
                    known => known,
                }
            }
        }

        impl std::str::FromStr for $name {
            type Err = std::convert::Infallible;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self::from(s))
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                Ok(Self::from(String::deserialize(deserializer)?))
            }
        }
    };
}

pub(crate) use string_enum;
