//
// Copyright 2026 Tabsdata Inc.
//

use const_format::concatcp;

use crate::base::constants::TABSDATA_LABEL_NAMESPACE;

pub(crate) const ANNOTATION_PROJECT: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "project");
pub(crate) const ANNOTATION_COLLECTION: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "collection");
pub(crate) const ANNOTATION_CONNECTION: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "connection");
pub(crate) const ANNOTATION_FUNCTION: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "function");
