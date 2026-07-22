//
// Copyright 2025 Tabsdata Inc.
//

use tm_workspace::workspace_root;

pub const TABSDATA_VERSION: &str =
    include_str!(concat!(workspace_root!(), "/assets/manifest/VERSION"));
