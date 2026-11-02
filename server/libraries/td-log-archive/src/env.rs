//
// Copyright 2026 Tabsdata Inc.
//

//! Where an instance keeps its worker logs, when not where the archive looks by
//! default. tdkserver sets these on the apiserver from `tabsdata.yaml`.

pub const ENV_TD_WORKER_LOGS_BUCKET: &str = "TD_WORKER_LOGS_BUCKET";
pub const ENV_TD_WORKER_LOGS_LOCATION: &str = "TD_WORKER_LOGS_LOCATION";
