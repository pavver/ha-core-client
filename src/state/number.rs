use super::string_enum;

string_enum! {
    /// Display mode of a `number` / `input_number` entity.
    pub enum NumberMode {
        Auto => "auto",
        Box => "box",
        Slider => "slider",
    }
}
