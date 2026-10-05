//
// Copyright 2026 Tabsdata Inc.
//

use const_format::concatcp;

use crate::base::constants::TABSDATA_LABEL_NAMESPACE;

pub(crate) const LABEL_PROJECT_ID: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "project-id");
pub(crate) const LABEL_COLLECTION_ID: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "collection-id");
pub(crate) const LABEL_CONNECTION_ID: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "connection-id");
pub(crate) const LABEL_FUNCTION_ID: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "function-id");
pub(crate) const LABEL_FUNCTION_VERSION_ID: &str =
    concatcp!(TABSDATA_LABEL_NAMESPACE, "function-version-id");
pub(crate) const LABEL_WORKER_ID: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "worker-id");

pub(crate) const LABEL_VALUE_MAX_LENGTH: usize = 63;

pub(crate) fn label_value(value: &str) -> String {
    let mapped: String = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
                character
            } else {
                '-'
            }
        })
        .collect();
    let truncated: String = mapped.chars().take(LABEL_VALUE_MAX_LENGTH).collect();
    truncated
        .trim_matches(|character: char| !character.is_ascii_alphanumeric())
        .to_owned()
}

#[cfg(test)]
mod tests;
