pub enum UpdateType {
    Add,
    Change
}

impl UpdateType {
    pub fn as_str(&self) -> &'static str {
        match self {
            UpdateType::Add => "A",
            UpdateType::Change => "C"
        }
    }
}
