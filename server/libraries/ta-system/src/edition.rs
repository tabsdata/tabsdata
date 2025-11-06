//
// Copyright 2025 Tabs Data Inc.
//

pub const OPEN_SOURCE_EDITION_LABEL: &str = "opensource";
pub const ENTERPRISE_EDITION_LABEL: &str = "enterprise";

pub trait Edition: Compatible {
    fn name(&self) -> &str;
    fn label(&self) -> &str;
    fn summary(&self) -> &str;
    fn enterprise(&self) -> bool;
}

pub trait Compatible {
    fn is_compatible(&self, label: &str) -> bool;
    fn requires_upgrade(&self, label: &str) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockEdition;

    impl Edition for MockEdition {
        fn name(&self) -> &str {
            "Mock Edition"
        }

        fn label(&self) -> &str {
            OPEN_SOURCE_EDITION_LABEL
        }

        fn summary(&self) -> &str {
            "This is a mock edition for testing."
        }

        fn enterprise(&self) -> bool {
            false
        }
    }

    impl Compatible for MockEdition {
        fn is_compatible(&self, label: &str) -> bool {
            label == ENTERPRISE_EDITION_LABEL
        }

        fn requires_upgrade(&self, _label: &str) -> bool {
            false
        }
    }

    #[test]
    fn test_edition_properties() {
        let edition = MockEdition;
        assert_eq!(edition.name(), "Mock Edition");
        assert_eq!(edition.label(), OPEN_SOURCE_EDITION_LABEL);
        assert_eq!(edition.summary(), "This is a mock edition for testing.");
    }

    #[test]
    fn test_compatibility() {
        let edition = MockEdition;
        assert!(!edition.is_compatible(OPEN_SOURCE_EDITION_LABEL));
        assert!(edition.is_compatible(ENTERPRISE_EDITION_LABEL));
        assert!(!edition.is_compatible("any_other_label"));
    }

    #[test]
    fn test_requires_upgrade() {
        let edition = MockEdition;
        assert!(!edition.requires_upgrade(OPEN_SOURCE_EDITION_LABEL));
        assert!(!edition.requires_upgrade(ENTERPRISE_EDITION_LABEL));
    }
}
