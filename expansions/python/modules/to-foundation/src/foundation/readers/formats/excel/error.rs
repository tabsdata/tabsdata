//
// Copyright 2026 Tabsdata Inc.
//

use arrow::error::ArrowError;

#[derive(Debug, thiserror::Error)]
pub enum ExcelError {
    #[error("excel: {0}")]
    Xlsx(#[from] calamine::XlsxError),

    #[error("excel sheet numbers start at 1 (got {0})")]
    SheetNumber(u32),

    #[error(
        "excel sheet {requested} is out of range: the workbook has {available} sheet(s), \
         counting hidden and chart sheets"
    )]
    SheetOutOfRange { requested: u32, available: usize },

    #[error("excel sheet {number} ('{name}') is a {kind} and holds no cells; name a worksheet")]
    NotAWorksheet {
        number: u32,
        name: String,
        kind: String,
    },

    #[error(transparent)]
    Arrow(#[from] ArrowError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, ExcelError>;
