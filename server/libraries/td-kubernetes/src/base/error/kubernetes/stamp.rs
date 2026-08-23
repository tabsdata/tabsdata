//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubeStampError {
    #[error("Could not resolve the version to stamp on the resource: {cause}.")]
    ResolveFailed { cause: String } = 5000,
}
