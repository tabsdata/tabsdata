//
// Copyright 2025 Tabs Data Inc.
//

use ta_system::edition::OPEN_SOURCE_EDITION_LABEL;
pub use ta_system::edition::{Compatible, Edition};

const EDITION_NAME: &str = "Tabsdata Open Source Edition";
const EDITION_SUMMARY: &str = "Tabsdata Open Source Edition";

pub struct TabsdataEdition;

impl Edition for TabsdataEdition {
    fn name(&self) -> &str {
        EDITION_NAME
    }

    fn label(&self) -> &str {
        OPEN_SOURCE_EDITION_LABEL
    }

    fn summary(&self) -> &str {
        EDITION_SUMMARY
    }

    fn enterprise(&self) -> bool {
        false
    }
}

impl Compatible for TabsdataEdition {
    fn is_compatible(&self, label: &str) -> bool {
        label == OPEN_SOURCE_EDITION_LABEL
    }

    fn requires_upgrade(&self, _label: &str) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_system::edition::ENTERPRISE_EDITION_LABEL;

    #[test]
    fn test_edition_properties() {
        let edition = TabsdataEdition;
        assert_eq!(edition.name(), "Tabsdata Open Source Edition");
        assert_eq!(edition.label(), OPEN_SOURCE_EDITION_LABEL);
        assert_eq!(edition.summary(), "Tabsdata Open Source Edition");
    }

    #[test]
    fn test_compatibility() {
        let edition = TabsdataEdition;
        assert!(edition.is_compatible(OPEN_SOURCE_EDITION_LABEL));
        assert!(!edition.is_compatible(ENTERPRISE_EDITION_LABEL));
        assert!(!edition.is_compatible("any_other_label"));
    }

    #[test]
    fn test_requires_upgrade() {
        let edition = TabsdataEdition;
        assert!(!edition.requires_upgrade(OPEN_SOURCE_EDITION_LABEL));
        assert!(!edition.requires_upgrade(ENTERPRISE_EDITION_LABEL));
        assert!(!edition.requires_upgrade("any_other_label"));
    }

    #[test]
    fn test_edition_enterprise() {
        let edition = TabsdataEdition;
        assert!(!edition.enterprise());
    }
}
