//
// Copyright 2025 Tabs Data Inc.
//

#[cfg(test)]
mod tests {
    use td_error::TdError;

    #[td_type::typed(string)]
    pub struct Id;

    #[td_type::typed(string)]
    pub struct Name;

    #[td_type::IdNameParam(param = "Foo", id = Id, name = Name)]
    struct FooParam;

    #[td_type::NestedParam]
    struct VarParam {
        foo: FooParam,
    }

    #[test]
    fn test_id() -> Result<(), TdError> {
        let param = VarParam {
            foo: FooParam::try_from("~foo_id".to_string())?,
        };
        assert_eq!(param.foo.id, Some("foo_id".try_into()?));
        assert_eq!(param.foo.name, None);
        let value = String::from(param.foo);
        assert_eq!(value, "~foo_id");
        Ok(())
    }

    #[test]
    fn test_name() -> Result<(), TdError> {
        let param = VarParam {
            foo: FooParam::try_from("foo_name".to_string())?,
        };
        assert_eq!(param.foo.id, None);
        assert_eq!(param.foo.name, Some("foo_name".try_into()?));
        let value = String::from(param.foo);
        assert_eq!(value, "foo_name");
        Ok(())
    }

    #[test]
    fn test_serde() -> Result<(), TdError> {
        let json = r#"{"foo": "~foo_id"}"#;
        let param: VarParam = serde_json::from_str(json).unwrap();
        assert_eq!(param.foo.id, Some("foo_id".try_into()?));
        assert_eq!(param.foo.name, None);

        let json = r#"{"foo": "foo_name"}"#;
        let param: VarParam = serde_json::from_str(json).unwrap();
        assert_eq!(param.foo.id, None);
        assert_eq!(param.foo.name, Some("foo_name".try_into()?));
        Ok(())
    }
}
