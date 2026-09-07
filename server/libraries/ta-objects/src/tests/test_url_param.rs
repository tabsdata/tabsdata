//
// Copyright 2025 Tabsdata Inc.
//

#[cfg(test)]
mod tests {
    use crate as ta_objects;
    use tm_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx, TypedTag};

    use td_error::TdError;

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    pub struct Id(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    pub struct Name(String);

    #[derive(TypedTag, TypedSerde, TypedOpenApi)]
    enum Foo {
        #[tag("~")]
        Id(Id),
        #[tag("")]
        Name(Name),
    }

    #[tm_objects::UrlParam]
    struct VarParam {
        foo: Foo,
    }

    #[test]
    fn test_id() -> Result<(), TdError> {
        let param = VarParam {
            foo: Foo::try_from("~foo_id".to_string())?,
        };
        assert!(matches!(&param.foo, Foo::Id(id) if id.to_string() == "foo_id"));
        let value = String::from(param.foo);
        assert_eq!(value, "~foo_id");
        Ok(())
    }

    #[test]
    fn test_name() -> Result<(), TdError> {
        let param = VarParam {
            foo: Foo::try_from("foo_name".to_string())?,
        };
        assert!(matches!(&param.foo, Foo::Name(name) if name.to_string() == "foo_name"));
        let value = String::from(param.foo);
        assert_eq!(value, "foo_name");
        Ok(())
    }

    #[test]
    fn test_serde() -> Result<(), TdError> {
        let json = r#"{"foo": "~foo_id"}"#;
        let param: VarParam = serde_json::from_str(json).unwrap();
        assert!(matches!(&param.foo, Foo::Id(id) if id.to_string() == "foo_id"));

        let json = r#"{"foo": "foo_name"}"#;
        let param: VarParam = serde_json::from_str(json).unwrap();
        assert!(matches!(&param.foo, Foo::Name(name) if name.to_string() == "foo_name"));
        Ok(())
    }
}
