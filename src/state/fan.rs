use super::string_enum;

string_enum! {
    /// Rotation direction of a fan.
    pub enum FanDirection {
        Forward => "forward",
        Reverse => "reverse",
    }
}
