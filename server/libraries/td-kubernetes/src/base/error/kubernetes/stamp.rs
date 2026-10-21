//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubeStampError {
    #[error("Could not resolve the version to stamp on the resource: {cause}.")]
    ResolveFailed { cause: String } = 5000,
    #[error(
        "Label '{label}' cannot take value '{value}': it is {length} characters long, but a \
         label value holds at most {maximum}."
    )]
    LabelValueTooLong {
        label: String,
        value: String,
        length: usize,
        maximum: usize,
    } = 5001,
}
