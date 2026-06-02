//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum CodecError {
    #[error("Could not decode encoded value: {0}.")]
    DecodeFailed(String) = 5000,
}
