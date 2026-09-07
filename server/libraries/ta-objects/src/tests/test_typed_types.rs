//
// Copyright 2025 Tabsdata Inc.
//

#[cfg(test)]
mod tests {
    use crate as ta_objects;

    use crate::SeaValue;
    use crate::typed::Typed as _;
    use crate::typed::TypedError;
    use td_common::datetime::Timestamp;
    use td_common::id::{Id, id};
    use td_common::time::UniqueUtc;
    use td_error::td_error;
    use tm_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};

    macro_rules! typed_test {
        ($type_:ty, $value:expr) => {
            paste::paste! {
                #[test]
                fn [< test_ $type_:lower >]() -> Result<(), td_error::TdError> {
                    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
                    #[typed(default = $value)]
                    struct TypedType($type_);

                    let typed = TypedType::default();
                    assert_eq!(*typed, $value);

                    let typed = TypedType::try_new($value)?;
                    assert_eq!(*typed, $value);

                    let typed: TypedType = $value.try_into()?;
                    assert_eq!(*typed, $value);
                    let serialized = serde_json::to_string(&typed).unwrap();
                    let deserialized: TypedType = serde_json::from_str(&serialized).unwrap();
                    assert_eq!(*deserialized, $value);

                    let display = format!("{}", typed);
                    assert_eq!(display, format!("{}", $value));
                    Ok(())
                }
            }
        };
    }

    // Default tests, testing defaults, parsing, deref, display, and serde.
    typed_test!(String, "typed string");
    typed_test!(i16, 15i16);
    typed_test!(i32, 15i32);
    typed_test!(i64, 15i64);
    typed_test!(f32, 15f32);
    typed_test!(f64, 15f64);

    // Complex types tests
    #[test]
    fn test_id() -> Result<(), td_error::TdError> {
        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        struct TypedType(Id);

        let _ = TypedType::default();

        let id = id();
        let typed = TypedType::try_new(id)?;
        assert_eq!(*typed, id);

        let serialized = serde_json::to_string(&typed).unwrap();
        let deserialized: TypedType = serde_json::from_str(&serialized).unwrap();
        assert_eq!(*deserialized, id);

        let display = format!("{typed}");
        assert_eq!(display, format!("{id}"));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_timestamp() -> Result<(), td_error::TdError> {
        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        struct TypedType(Timestamp);

        let before = UniqueUtc::now_millis();
        let typed = TypedType::default();
        let after = UniqueUtc::now_millis();
        assert!(**typed > before);
        assert!(after > **typed);

        let typed = TypedType::try_new(chrono::DateTime::<chrono::Utc>::default())?;
        assert_eq!(**typed, chrono::DateTime::<chrono::Utc>::default());

        let typed: TypedType = chrono::DateTime::<chrono::Utc>::default().into();
        assert_eq!(**typed, chrono::DateTime::<chrono::Utc>::default());
        let serialized = serde_json::to_string(&typed).unwrap();
        let deserialized: TypedType = serde_json::from_str(&serialized).unwrap();
        assert_eq!(**deserialized, chrono::DateTime::<chrono::Utc>::default());

        let display = format!("{typed}");
        assert_eq!(
            display,
            format!(
                "{}",
                chrono::DateTime::<chrono::Utc>::default().timestamp_millis()
            )
        );
        Ok(())
    }

    macro_rules! default_const_typed_test {
        ($type_:ty, $default:tt) => {
            paste::paste! {
                #[test]
                fn [< test_ $type_:lower _default_const >]() -> Result<(), td_error::TdError> {
                    const DEFAULT: $type_ = $default;

                    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
                    #[typed(default = DEFAULT)]
                    struct TypedType($type_);

                    let typed = TypedType::default();
                    assert_eq!(*typed, $default);
                    Ok(())
                }
            }
        };

        ($type_:ty, $const_type:ty, $default:tt) => {
            paste::paste! {
                #[test]
                fn [< test_ $type_:lower _default_const >]() -> Result<(), td_error::TdError> {
                    const DEFAULT: $const_type = $default;

                    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
                    #[typed(default = DEFAULT)]
                    struct TypedType($type_);

                    let typed = TypedType::default();
                    assert_eq!(*typed, $default);
                    Ok(())
                }
            }
        };
    }

    macro_rules! default_literal_and_closure_typed_test {
        ($type_:ty, $default:tt) => {
            paste::paste! {
                #[test]
                fn [< test_ $type_:lower _default >]() -> Result<(), td_error::TdError> {
                    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
                    #[typed(default = $default)]
                    struct TypedType($type_);

                    let typed = TypedType::default();
                    assert_eq!(*typed, $default);
                    Ok(())
                }

                #[test]
                fn [< test_ $type_:lower _default_closure >]() -> Result<(), td_error::TdError> {
                    fn default() -> $type_ {
                        $default.into()
                    }

                    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
                    #[typed(default = default())]
                    struct TypedType($type_);

                    let typed = TypedType::default();
                    assert_eq!(*typed, default());
                    Ok(())
                }
            }
        };
    }

    macro_rules! default_typed_test {
        ($type_:ty, $default:tt) => {
            paste::paste! {
                default_const_typed_test!($type_, $default);
                default_literal_and_closure_typed_test!($type_, $default);
            }
        };

        ($type_:ty, $const_type:ty, $default:tt) => {
            default_const_typed_test!($type_, $const_type, $default);
            default_literal_and_closure_typed_test!($type_, $default);
        };
    }

    // Testing if const, literal, and closure defaults work.
    default_typed_test!(String, &str, "typed string");
    default_typed_test!(i16, 15i16);
    default_typed_test!(i32, 15i32);
    default_typed_test!(i64, 15i64);
    default_typed_test!(f32, 15f32);
    default_typed_test!(f64, 15f64);
    default_typed_test!(bool, true);

    #[test]
    fn test_id_default() {
        fn default_id() -> Id {
            Id::try_from("00000000000000000000000000").unwrap()
        }

        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        #[typed(default = default_id())]
        struct TypedId(Id);

        assert_eq!(*TypedId::default(), default_id());
    }

    #[test]
    fn test_timestamp_default() {
        let before = UniqueUtc::now_millis();

        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        #[typed(default = Timestamp::default())]
        struct TypedTimestamp(Timestamp);

        let default = TypedTimestamp::default();

        let after = UniqueUtc::now_millis();
        assert!(**default >= before);
        assert!(after >= **default);
    }

    macro_rules! try_from_typed_test {
        ($type_:ty, $default:tt) => {
            paste::paste! {
                #[test]
                fn [< test_ $type_:lower _try_from >]() -> Result<(), td_error::TdError> {
                    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
                    #[typed(default = $default)]
                    struct TypedType($type_);

                    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
                    #[typed(try_from = TypedType)]
                    struct NewTypedType($type_);

                    let typed = TypedType::default();
                    let new_typed: NewTypedType = typed.clone().try_into()?;
                    assert_eq!(*new_typed, *typed);
                    Ok(())
                }
            }
        };
    }

    // Testing try_from implementations between same types.
    try_from_typed_test!(String, "typed string");
    try_from_typed_test!(i16, 15i16);
    try_from_typed_test!(i32, 15i32);
    try_from_typed_test!(i64, 15i64);
    try_from_typed_test!(f32, 15f32);
    try_from_typed_test!(f64, 15f64);
    try_from_typed_test!(bool, true);

    // Testing try_from implementations between different types (impl TryFrom inner types
    // must be implemented so this works).

    #[test]
    fn test_i16_i32_try_from() -> Result<(), td_error::TdError> {
        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        struct TypedType(i16);

        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        #[typed(try_from = TypedType)]
        struct NewTypedType(i32);

        let typed = TypedType::default();
        let new_typed: NewTypedType = typed.try_into()?;
        assert_eq!(*new_typed as i16, *typed);
        Ok(())
    }

    #[test]
    fn test_id_string_try_from() -> Result<(), td_error::TdError> {
        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        struct TypedType(Id);

        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        #[typed(try_from = TypedType)]
        struct NewTypedType(String);

        let typed = TypedType::default();
        let _new_typed: NewTypedType = typed.try_into()?;
        // We just care that the into is possible as Id is a mock type.
        Ok(())
    }

    // Other String tests (min_len, max_len, len, regex, parser)
    #[test]
    fn test_string_min_len() {
        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        #[typed(min_len = 5)]
        struct TypedString(String);

        assert!(TypedString::try_new("1234").is_err());
        assert!(TypedString::try_new("12345").is_ok());
        assert!(TypedString::try_new("123456").is_ok());
    }

    #[test]
    fn test_string_max_len() {
        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        #[typed(max_len = 5)]
        struct TypedString(String);

        assert!(TypedString::try_new("1234").is_ok());
        assert!(TypedString::try_new("12345").is_ok());
        assert!(TypedString::try_new("123456").is_err());
    }

    #[test]
    fn test_string_min_max_len() {
        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        #[typed(min_len = 4, max_len = 5)]
        struct TypedString(String);

        assert!(TypedString::try_new("123").is_err());
        assert!(TypedString::try_new("1234").is_ok());
        assert!(TypedString::try_new("12345").is_ok());
        assert!(TypedString::try_new("123456").is_err());
    }

    #[test]
    fn test_string_len() {
        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        #[typed(len = 5)]
        struct TypedString(String);

        assert!(TypedString::try_new("1234").is_err());
        assert!(TypedString::try_new("12345").is_ok());
        assert!(TypedString::try_new("123456").is_err());
    }

    #[test]
    fn test_string_len_const() {
        const LEN_CONST: usize = 5;

        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        #[typed(len = LEN_CONST)]
        struct TypedString(String);

        assert!(TypedString::try_new("1234").is_err());
        assert!(TypedString::try_new("12345").is_ok());
        assert!(TypedString::try_new("123456").is_err());
    }

    #[test]
    fn test_string_len_fn() {
        fn len() -> usize {
            5
        }

        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        #[typed(len = len())]
        struct TypedString(String);

        assert!(TypedString::try_new("1234").is_err());
        assert!(TypedString::try_new("12345").is_ok());
        assert!(TypedString::try_new("123456").is_err());
    }

    #[test]
    fn test_string_regex() {
        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        #[typed(regex = "^[0-9]")]
        struct TypedString(String);

        assert!(TypedString::try_new("abc").is_err());
        assert!(TypedString::try_new("12345").is_ok());
    }

    #[test]
    fn test_string_regex_const() {
        const NAME_REGEX: &str = "^[0-9]";

        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        #[typed(regex = NAME_REGEX)]
        struct TypedString(String);

        assert!(TypedString::try_new("abc").is_err());
        assert!(TypedString::try_new("12345").is_ok());
    }

    #[td_error]
    enum ParsingError {
        #[error("parse error: {0}, {1}")]
        Parse(String, String) = 0,
    }

    #[test]
    fn test_string_parser() {
        fn parser(s: String) -> Result<String, td_error::TdError> {
            if s.starts_with("123") {
                Ok(s)
            } else {
                Err(ParsingError::Parse(s, "must start with 123".to_string()))?
            }
        }

        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        #[typed(parser = parser)]
        struct TypedString(String);

        assert!(TypedString::try_new("1234").is_ok());
        assert!(TypedString::try_new("12345").is_ok());
        assert!(TypedString::try_new("456").is_err());
    }

    #[test]
    fn test_string_parser_closure() {
        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        #[typed(parser = |s| {
            if s.starts_with("123") {
                Ok(s)
            } else {
                Err(ParsingError::Parse(s, "must start with 123".to_string()))?
            }
        })]
        struct TypedString(String);

        assert!(TypedString::try_new("1234").is_ok());
        assert!(TypedString::try_new("12345").is_ok());
        assert!(TypedString::try_new("456").is_err());
    }

    // Numeric tests
    macro_rules! min_max_typed_numeric_test {
        ($type_:ty, $default:expr) => {
            paste::paste! {
                #[test]
                fn [< test_ $type_:lower _default_numeric >]() -> Result<(), td_error::TdError> {
                    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
                    #[typed(min = [< 2 $type_:lower >], max = [< 4 $type_:lower >])]
                    struct TypedType($type_);

                    assert!(matches!(
                        TypedType::try_new([< 1 $type_:lower >]).unwrap_err().domain_err::<TypedError>(),
                        TypedError::Min(..)
                    ));
                    assert!(TypedType::try_new([< 2 $type_:lower >]).is_ok());
                    assert!(TypedType::try_new([< 3 $type_:lower >]).is_ok());
                    assert!(TypedType::try_new([< 4 $type_:lower >]).is_ok());
                    assert!(matches!(
                        TypedType::try_new([< 5 $type_:lower >]).unwrap_err().domain_err::<TypedError>(),
                        TypedError::Max(..)
                    ));
                    assert!(matches!(
                        TypedType::try_new([< 6 $type_:lower >]).unwrap_err().domain_err::<TypedError>(),
                        TypedError::Max(..)
                    ));
                    Ok(())
                }
            }
        };
    }

    min_max_typed_numeric_test!(i16, 15i16);
    min_max_typed_numeric_test!(i32, 15i32);
    min_max_typed_numeric_test!(i64, 15i64);
    min_max_typed_numeric_test!(f32, 15f32);
    min_max_typed_numeric_test!(f64, 15f64);

    #[test]
    fn test_sqlx() {
        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        struct TypedType(String);

        fn assert_sea_value<T: SeaValue>() {}
        assert_sea_value::<TypedType>();
    }

    // ---- SeaValue for basic typed types --------------------------------------

    #[test]
    fn test_sea_value_string() {
        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        struct MyStr(String);

        let v = MyStr::try_from("hello").unwrap();
        assert_eq!(
            v.as_sea_value(),
            sea_query::Value::String(Some("hello".into()))
        );
    }

    #[test]
    fn test_sea_value_i32() {
        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        struct MyI32(i32);

        let v = MyI32::try_from(42i32).unwrap();
        assert_eq!(v.as_sea_value(), sea_query::Value::Int(Some(42)));
    }

    #[test]
    fn test_sea_value_i64() {
        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        struct MyI64(i64);

        let v = MyI64::try_from(99i64).unwrap();
        assert_eq!(v.as_sea_value(), sea_query::Value::BigInt(Some(99)));
    }

    #[test]
    fn test_sea_value_bool() {
        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        struct MyBool(bool);

        let v = MyBool::TRUE;
        assert_eq!(v.as_sea_value(), sea_query::Value::Bool(Some(true)));
        let v = MyBool::FALSE;
        assert_eq!(v.as_sea_value(), sea_query::Value::Bool(Some(false)));
    }

    #[test]
    fn test_sea_value_f64() {
        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        struct MyF64(f64);

        let v = MyF64::try_from(2.72f64).unwrap();
        assert_eq!(v.as_sea_value(), sea_query::Value::Double(Some(2.72)));
    }

    // ---- NullableSeaValue for basic typed types ------------------------------

    #[test]
    fn test_nullable_sea_value_string() {
        use crate::NullableSeaValue;

        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        struct NStr(String);

        assert_eq!(NStr::null_sea_value(), sea_query::Value::String(None));
    }

    #[test]
    fn test_nullable_sea_value_i64() {
        use crate::NullableSeaValue;

        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        struct NI64(i64);

        assert_eq!(NI64::null_sea_value(), sea_query::Value::BigInt(None));
    }

    #[test]
    fn test_nullable_sea_value_bool() {
        use crate::NullableSeaValue;

        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        struct NBool(bool);

        assert_eq!(NBool::null_sea_value(), sea_query::Value::Bool(None));
    }

    // ---- Option<T> SeaValue --------------------------------------------------

    #[test]
    fn test_option_sea_value_some() {
        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        struct OptStr(String);

        let v: Option<OptStr> = Some(OptStr::try_from("val").unwrap());
        assert_eq!(
            v.as_sea_value(),
            sea_query::Value::String(Some("val".into()))
        );
    }

    #[test]
    fn test_option_sea_value_none() {
        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        struct OptStr2(String);

        let v: Option<OptStr2> = None;
        assert_eq!(v.as_sea_value(), sea_query::Value::String(None));
    }

    // ---- SqlCondition for basic typed types ----------------------------------

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct CondStr(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct CondI64(i64);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct CondBool(bool);

    #[ta_objects::Dao]
    #[dao(table = "test_cond", ops(select, insert, delete))]
    struct CondDao {
        col: CondStr,
        num: CondI64,
        flag: CondBool,
    }

    fn cond_sql(cond: impl crate::sql_condition::SqlCondition<CondDao>) -> String {
        let cond = cond.resolve();
        let q = sea_query::Query::select()
            .column(CondDaoIden::Col)
            .from(CondDaoIden::Table)
            .cond_where(cond)
            .to_string(sea_query::PostgresQueryBuilder);
        match q.find("WHERE ") {
            Some(pos) => q[pos + 6..].to_string(),
            None => "TRUE".to_string(),
        }
    }

    #[test]
    fn test_sql_condition_string_type() {
        let v = CondStr::try_from("abc").unwrap();
        assert_eq!(cond_sql(&v), r#""col" = 'abc'"#);
    }

    #[test]
    fn test_sql_condition_i64_type() {
        let v = CondI64::try_from(42i64).unwrap();
        assert_eq!(cond_sql(v), r#""num" = 42"#);
    }

    #[test]
    fn test_sql_condition_bool_type() {
        let v = CondBool::TRUE;
        assert_eq!(cond_sql(v), r#""flag" = TRUE"#);
    }

    // ---- Compact Debug + secret tests ---------------------------------------

    #[test]
    fn test_string_compact_debug() {
        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        struct PublicString(String);

        let s = PublicString::try_new("visible").unwrap();
        // Debug delegates to inner value, no newtype wrapper shown
        assert_eq!(format!("{:?}", s), r#""visible""#);
        assert_eq!(format!("{:#?}", s), r#""visible""#);
    }

    #[test]
    fn test_string_secret_debug() {
        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        #[typed(secret)]
        struct SecretString(String);

        let s = SecretString::try_new("my_secret_value").unwrap();
        assert_eq!(format!("{:?}", s), "***");
        assert!(!format!("{:?}", s).contains("my_secret_value"));
        // Inner value still accessible via Deref
        assert_eq!(&*s, "my_secret_value");
    }
}
