//
// Copyright 2025. Tabs Data Inc.
//

use crate::types::basic::PythonVersion;

#[td_type::Dto]
pub struct RuntimeInfo {
    #[builder(default)]
    versions: Vec<PythonVersion>,
}
