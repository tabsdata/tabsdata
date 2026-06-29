//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum SchemaError {
    #[error("Could not parse schema '{schema}': {cause}.")]
    ParseFailed { schema: String, cause: String } = 5040,
    #[error("Schema '{schema}' has an empty 'at' pattern.")]
    EmptyPattern { schema: String } = 5041,
    #[error("Schema '{schema}' rule has an unsupported treatment '{treatment}'.")]
    UnsupportedTreatment { schema: String, treatment: String } = 5042,
}
