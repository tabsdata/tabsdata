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

    #[td_type::typed(id_name(id = Id, name = Name))]
    struct Foo;

    #[test]
    fn test_id() -> Result<(), TdError> {
        let param = Foo::try_from("~foo_id".to_string())?;
        assert_eq!(param.id, Some("foo_id".try_into()?));
        assert_eq!(param.name, None);
        let value = String::from(param);
        assert_eq!(value, "~foo_id");
        Ok(())
    }

    #[test]
    fn test_name() -> Result<(), TdError> {
        let param = Foo::try_from("foo_name".to_string())?;
        assert_eq!(param.id, None);
        assert_eq!(param.name, Some("foo_name".try_into()?));
        let value = String::from(param);
        assert_eq!(value, "foo_name");
        Ok(())
    }

    #[test]
    fn test_serde() -> Result<(), TdError> {
        let json = r#""~foo_id""#;
        let param: Foo = serde_json::from_str(json).unwrap();
        assert_eq!(param.id, Some("foo_id".try_into()?));
        assert_eq!(param.name, None);

        let json = r#""foo_name""#;
        let param: Foo = serde_json::from_str(json).unwrap();
        assert_eq!(param.id, None);
        assert_eq!(param.name, Some("foo_name".try_into()?));
        Ok(())
    }
}
