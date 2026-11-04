//
// Copyright 2026 Tabsdata Inc.
//

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExcelOptions {
    pub sheet: u32,
    pub has_header: bool,
}

impl Default for ExcelOptions {
    fn default() -> Self {
        Self {
            sheet: 1,
            has_header: true,
        }
    }
}
