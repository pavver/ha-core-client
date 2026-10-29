use super::string_enum;

string_enum! {
    /// How a `device_tracker` entity learns its location.
    pub enum SourceType {
        Gps => "gps",
        Router => "router",
        Bluetooth => "bluetooth",
        BluetoothLe => "bluetooth_le",
    }
}
