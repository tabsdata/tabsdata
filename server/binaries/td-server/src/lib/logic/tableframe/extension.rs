//
// Copyright 2024 Tabs Data Inc.
//

#[cfg(test)]
pub mod tests {
    use ta_tableframe::api::Extension;
    use te_tableframe::engine::TableframeExtension;

    #[test]
    #[cfg(not(feature = "enterprise"))]
    fn test_summary_annotation() {
        assert_eq!(
            TableframeExtension.summary().unwrap(),
            "te-tableframe-standard".to_string()
        )
    }

    #[test]
    #[cfg(feature = "enterprise")]
    fn test_summary_annotation() {
        assert_eq!(
            TableframeExtension.summary().unwrap(),
            "te-tableframe-enterprise".to_string()
        )
    }

    #[test]
    fn test_summary_condition() {
        let summary = TableframeExtension.summary().unwrap();
        if !cfg!(feature = "enterprise") {
            assert_eq!(summary, "te-tableframe-standard".to_string())
        } else {
            assert_eq!(summary, "te-tableframe-enterprise".to_string())
        }
    }
}
