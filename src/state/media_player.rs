use super::string_enum;

string_enum! {
    /// Repeat mode of a media player.
    pub enum RepeatMode {
        Off => "off",
        All => "all",
        One => "one",
    }
}
