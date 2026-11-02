//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

pub mod services;

#[td_error]
pub enum UiDataError {
    #[error("Invalid 'override' value '{0}': expected 'true' or 'false'")]
    InvalidOverride(String) = 0,
    #[error("If-Match must hold exactly one ETag; '*' and lists are not supported")]
    UnsupportedIfMatch = 1,
    #[error("The UI data document is empty")]
    EmptyDocument = 2,
    #[error("The UI data document is not valid UTF-8")]
    DocumentNotUtf8 = 3,
    #[error("The UI data document contains a NUL character")]
    DocumentHasNul = 4,

    #[error("No UI data is stored for this user")]
    UiDataNotFound = 1000,
}
