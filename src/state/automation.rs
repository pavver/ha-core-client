use super::string_enum;

string_enum! {
    /// Run mode of an automation (what happens when it triggers while running).
    pub enum RunMode {
        Single => "single",
        Restart => "restart",
        Queued => "queued",
        Parallel => "parallel",
    }
}
