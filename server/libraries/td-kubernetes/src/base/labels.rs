//
// Copyright 2026 Tabs Data Inc.
//

use const_format::concatcp;

use crate::base::constants::TABSDATA_LABEL_NAMESPACE;

pub(crate) const LABEL_PROJECT_ID: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "project-id");
pub(crate) const LABEL_COLLECTION_ID: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "collection-id");
pub(crate) const LABEL_COLLECTION: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "collection");
pub(crate) const LABEL_CONNECTION_ID: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "connection-id");
pub(crate) const LABEL_FUNCTION_ID: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "function-id");
pub(crate) const LABEL_FUNCTION: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "function");
pub(crate) const LABEL_FUNCTION_VERSION_ID: &str =
    concatcp!(TABSDATA_LABEL_NAMESPACE, "function-version-id");
pub(crate) const LABEL_WORKER_ID: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "worker-id");
