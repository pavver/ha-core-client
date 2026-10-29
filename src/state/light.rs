use super::string_enum;

string_enum! {
    /// Color mode of a light (`color_mode` / `supported_color_modes` attributes).
    pub enum ColorMode {
        Unknown => "unknown",
        OnOff => "onoff",
        Brightness => "brightness",
        ColorTemp => "color_temp",
        Hs => "hs",
        Xy => "xy",
        Rgb => "rgb",
        Rgbw => "rgbw",
        Rgbww => "rgbww",
        White => "white",
    }
}
