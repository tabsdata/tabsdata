//
// Copyright 2026 Tabsdata Inc.
//

pub trait ComposedString: Sized {
    fn parse(s: impl Into<String>) -> Result<Self, td_error::TdError>;
    fn compose(&self) -> String;
}
