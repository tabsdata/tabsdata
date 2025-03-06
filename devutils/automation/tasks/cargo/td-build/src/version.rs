//
// Copyright 2025 Tabs Data Inc.
//

use tm_workspace::workspace_root;

pub const TABSDATA_VERSION: &str =
    include_str!(concat!(workspace_root!(), "/assets/manifest/VERSION"));
