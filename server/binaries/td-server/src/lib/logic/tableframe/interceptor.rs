//
// Copyright 2024 Tabs Data Inc.
//

#[cfg(test)]
pub mod tests {
    use td_interceptor::engine::Interceptor;
    use td_interceptor_api::api::InterceptorPlugin;

    #[test]
    #[cfg(not(feature = "enterprise"))]
    fn test_summary_annotation() {
        assert_eq!(
            Interceptor.summary().unwrap(),
            "td-interceptor-standard".to_string()
        )
    }

    #[test]
    #[cfg(feature = "enterprise")]
    fn test_summary_annotation() {
        assert_eq!(
            Interceptor.summary().unwrap(),
            "td-interceptor-enterprise".to_string()
        )
    }

    #[test]
    fn test_summary_condition() {
        let summary = Interceptor.summary().unwrap();
        if !cfg!(feature = "enterprise") {
            assert_eq!(summary, "td-interceptor-standard".to_string())
        } else {
            assert_eq!(summary, "td-interceptor-enterprise".to_string())
        }
    }
}
