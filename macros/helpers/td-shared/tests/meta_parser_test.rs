//
//  Copyright 2024 Tabs Data Inc.
//

#[cfg(test)]
mod tests {
    extern crate proc_macro;

    use darling::FromMeta;
    use proc_macro2::TokenStream;
    use quote::quote;

    use td_shared::parse_meta;

    #[derive(Debug, FromMeta, PartialEq)]
    struct TestMeta {
        name: String,
        #[darling(default)]
        version: Option<String>,
    }

    #[test]
    fn test_parse_meta_success() {
        let args: TokenStream = quote! { name = "example", version = "1.0" };
        let result: TestMeta = parse_meta!(TestMeta, args).unwrap();
        let expected = TestMeta {
            name: "example".to_string(),
            version: Some("1.0".to_string()),
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_meta_missing_optional_field() {
        let args: TokenStream = quote! { name = "example" };
        let result: TestMeta = parse_meta!(TestMeta, args).unwrap();
        let expected = TestMeta {
            name: "example".to_string(),
            version: None,
        };
        assert_eq!(result, expected);
    }
}
