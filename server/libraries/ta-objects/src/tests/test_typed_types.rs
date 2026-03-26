//
// Copyright 2025 Tabs Data Inc.
//

#[cfg(test)]
mod tests {
    use crate as ta_objects;

    use crate::SeaValue;
    use td_common::datetime::Timestamp;
    use td_common::id::{Id, id};
    use td_common::time::UniqueUtc;
    use td_error::td_error;

    macro_rules! typed_test {
        ($type_:ty, $value:expr) => {
            paste::paste! {
                #[test]
                fn [< test_ $type_:lower >]() -> Result<(), td_error::TdError> {
                    #[ta_objects::typed([< $type_:lower >](default = $value))]
                    struct TypedType;

                    let typed = TypedType::default();
                    assert_eq!(*typed, $value);

                    let typed = TypedType::parse($value)?;
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
        #[ta_objects::typed(id)]
        struct TypedType;

        let _ = TypedType::default();

        let id = id();
        let typed = TypedType::parse(id)?;
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
        #[ta_objects::typed(timestamp)]
        struct TypedType;

        let before = UniqueUtc::now_millis();
        let typed = TypedType::default();
        let after = UniqueUtc::now_millis();
        assert!(**typed > before);
        assert!(after > **typed);

        let typed = TypedType::parse(chrono::DateTime::<chrono::Utc>::default())?;
        assert_eq!(**typed, chrono::DateTime::<chrono::Utc>::default());

        let typed: TypedType = chrono::DateTime::<chrono::Utc>::default().try_into()?;
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

                    #[ta_objects::typed([< $type_:lower >](default = DEFAULT))]
                    struct TypedType;

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

                    #[ta_objects::typed([< $type_:lower >](default = DEFAULT))]
                    struct TypedType;

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
                    #[ta_objects::typed([< $type_:lower >](default = $default))]
                    struct TypedType;

                    let typed = TypedType::default();
                    assert_eq!(*typed, $default);
                    Ok(())
                }

                #[test]
                fn [< test_ $type_:lower _default_closure >]() -> Result<(), td_error::TdError> {
                    fn default() -> $type_ {
                        $default.into()
                    }

                    #[ta_objects::typed([< $type_:lower >](default = default()))]
                    struct TypedType;

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

        #[ta_objects::typed(id(default = default_id()))]
        struct TypedId;

        assert_eq!(
            Id::try_from("00000000000000000000000000").unwrap(),
            default_id()
        );
    }

    #[test]
    fn test_timestamp_default() {
        let before = UniqueUtc::now_millis();

        #[ta_objects::typed(timestamp(default = Timestamp::default()))]
        struct TypedTimestamp;

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
                    #[ta_objects::typed([< $type_:lower >](default = $default))]
                    struct TypedType;

                    #[ta_objects::typed([< $type_:lower >], try_from = TypedType)]
                    struct NewTypedType;

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
    // bool typed types don't generate `parse`, so `try_from` is not supported.

    // Testing try_from implementations between different types (impl TryFrom inner types
    // must be implemented so this works).

    #[test]
    fn test_i16_i32_try_from() -> Result<(), td_error::TdError> {
        #[ta_objects::typed(i16)]
        struct TypedType;

        #[ta_objects::typed(i32, try_from = TypedType)]
        struct NewTypedType;

        let typed = TypedType::default();
        let new_typed: NewTypedType = typed.try_into()?;
        assert_eq!(*new_typed as i16, *typed);
        Ok(())
    }

    #[test]
    fn test_id_string_try_from() -> Result<(), td_error::TdError> {
        #[ta_objects::typed(id)]
        struct TypedType;

        #[ta_objects::typed(string, try_from = TypedType)]
        struct NewTypedType;

        let typed = TypedType::default();
        let _new_typed: NewTypedType = typed.try_into()?;
        // We just care that the into is possible as Id is a mock type.
        Ok(())
    }

    // Other String tests (min_len, max_len, len, regex, parser)
    #[test]
    fn test_string_min_len() {
        #[ta_objects::typed(string(min_len = 5))]
        struct TypedString;

        assert!(TypedString::parse("1234").is_err());
        assert!(TypedString::parse("12345").is_ok());
        assert!(TypedString::parse("123456").is_ok());
    }

    #[test]
    fn test_string_max_len() {
        #[ta_objects::typed(string(max_len = 5))]
        struct TypedString;

        assert!(TypedString::parse("1234").is_ok());
        assert!(TypedString::parse("12345").is_ok());
        assert!(TypedString::parse("123456").is_err());
    }

    #[test]
    fn test_string_min_max_len() {
        #[ta_objects::typed(string(min_len = 4, max_len = 5))]
        struct TypedString;

        assert!(TypedString::parse("123").is_err());
        assert!(TypedString::parse("1234").is_ok());
        assert!(TypedString::parse("12345").is_ok());
        assert!(TypedString::parse("123456").is_err());
    }

    #[test]
    fn test_string_len() {
        #[ta_objects::typed(string(len = 5))]
        struct TypedString;

        assert!(TypedString::parse("1234").is_err());
        assert!(TypedString::parse("12345").is_ok());
        assert!(TypedString::parse("123456").is_err());
    }

    #[test]
    fn test_string_len_const() {
        const LEN_CONST: usize = 5;

        #[ta_objects::typed(string(len = LEN_CONST))]
        struct TypedString;

        assert!(TypedString::parse("1234").is_err());
        assert!(TypedString::parse("12345").is_ok());
        assert!(TypedString::parse("123456").is_err());
    }

    #[test]
    fn test_string_len_fn() {
        fn len() -> usize {
            5
        }

        #[ta_objects::typed(string(len = len()))]
        struct TypedString;

        assert!(TypedString::parse("1234").is_err());
        assert!(TypedString::parse("12345").is_ok());
        assert!(TypedString::parse("123456").is_err());
    }

    #[test]
    fn test_string_regex() {
        #[ta_objects::typed(string(regex = "^[0-9]"))]
        struct TypedString;

        assert!(TypedString::parse("abc").is_err());
        assert!(TypedString::parse("12345").is_ok());
    }

    #[test]
    fn test_string_regex_const() {
        const NAME_REGEX: &str = "^[0-9]";

        #[ta_objects::typed(string(regex = NAME_REGEX))]
        struct TypedString;

        assert!(TypedString::parse("abc").is_err());
        assert!(TypedString::parse("12345").is_ok());
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

        #[ta_objects::typed(string(parser = parser))]
        struct TypedString;

        assert!(TypedString::parse("1234").is_ok());
        assert!(TypedString::parse("12345").is_ok());
        assert!(TypedString::parse("456").is_err());
    }

    #[test]
    fn test_string_parser_closure() {
        #[ta_objects::typed(string(parser = |s| {
            if s.starts_with("123") {
                Ok(s)
            } else {
                Err(ParsingError::Parse(s, "must start with 123".to_string()))?
            }
        }))]
        struct TypedString;

        assert!(TypedString::parse("1234").is_ok());
        assert!(TypedString::parse("12345").is_ok());
        assert!(TypedString::parse("456").is_err());
    }

    // Numeric tests
    macro_rules! min_max_typed_numeric_test {
        ($type_:ty, $default:expr) => {
            paste::paste! {
                #[test]
                fn [< test_ $type_:lower _default_numeric >]() -> Result<(), td_error::TdError> {
                    #[ta_objects::typed([< $type_:lower >](min = [< 2 $type_:lower >], max = [< 4 $type_:lower >]))]
                    struct TypedType;

                    assert!(matches!(TypedType::parse([< 1 $type_:lower >]), Err(TypedTypeError::Min(_, _))));
                    assert!(TypedType::parse([< 2 $type_:lower >]).is_ok());
                    assert!(TypedType::parse([< 3 $type_:lower >]).is_ok());
                    assert!(TypedType::parse([< 4 $type_:lower >]).is_ok());
                    assert!(matches!(TypedType::parse([< 5 $type_:lower >]), Err(TypedTypeError::Max(_, _))));
                    assert!(matches!(TypedType::parse([< 6 $type_:lower >]), Err(TypedTypeError::Max(_, _))));
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
        #[ta_objects::typed(string)]
        struct TypedType;

        fn assert_sea_value<T: SeaValue>() {}
        assert_sea_value::<TypedType>();
    }

    // ---- SeaValue for basic typed types --------------------------------------

    #[test]
    fn test_sea_value_string() {
        #[ta_objects::typed(string)]
        struct MyStr;

        let v = MyStr::try_from("hello").unwrap();
        assert_eq!(
            v.as_sea_value(),
            sea_query::Value::String(Some("hello".into()))
        );
        assert_eq!(v.as_type_id(), std::any::TypeId::of::<MyStr>());
    }

    #[test]
    fn test_sea_value_i32() {
        #[ta_objects::typed(i32)]
        struct MyI32;

        let v = MyI32::try_from(42i32).unwrap();
        assert_eq!(v.as_sea_value(), sea_query::Value::Int(Some(42)));
        assert_eq!(v.as_type_id(), std::any::TypeId::of::<MyI32>());
    }

    #[test]
    fn test_sea_value_i64() {
        #[ta_objects::typed(i64)]
        struct MyI64;

        let v = MyI64::try_from(99i64).unwrap();
        assert_eq!(v.as_sea_value(), sea_query::Value::BigInt(Some(99)));
        assert_eq!(v.as_type_id(), std::any::TypeId::of::<MyI64>());
    }

    #[test]
    fn test_sea_value_bool() {
        #[ta_objects::typed(bool)]
        struct MyBool;

        let v = MyBool::from(true);
        assert_eq!(v.as_sea_value(), sea_query::Value::Bool(Some(true)));
        let v = MyBool::from(false);
        assert_eq!(v.as_sea_value(), sea_query::Value::Bool(Some(false)));
        assert_eq!(v.as_type_id(), std::any::TypeId::of::<MyBool>());
    }

    #[test]
    fn test_sea_value_f64() {
        #[ta_objects::typed(f64)]
        struct MyF64;

        let v = MyF64::try_from(3.14f64).unwrap();
        assert_eq!(v.as_sea_value(), sea_query::Value::Double(Some(3.14)));
    }

    // ---- NullableSeaValue for basic typed types ------------------------------

    #[test]
    fn test_nullable_sea_value_string() {
        use crate::NullableSeaValue;

        #[ta_objects::typed(string)]
        struct NStr;

        assert_eq!(NStr::null_sea_value(), sea_query::Value::String(None));
    }

    #[test]
    fn test_nullable_sea_value_i64() {
        use crate::NullableSeaValue;

        #[ta_objects::typed(i64)]
        struct NI64;

        assert_eq!(NI64::null_sea_value(), sea_query::Value::BigInt(None));
    }

    #[test]
    fn test_nullable_sea_value_bool() {
        use crate::NullableSeaValue;

        #[ta_objects::typed(bool)]
        struct NBool;

        assert_eq!(NBool::null_sea_value(), sea_query::Value::Bool(None));
    }

    // ---- Option<T> SeaValue --------------------------------------------------

    #[test]
    fn test_option_sea_value_some() {
        #[ta_objects::typed(string)]
        struct OptStr;

        let v: Option<OptStr> = Some(OptStr::try_from("val").unwrap());
        assert_eq!(
            v.as_sea_value(),
            sea_query::Value::String(Some("val".into()))
        );
    }

    #[test]
    fn test_option_sea_value_none() {
        #[ta_objects::typed(string)]
        struct OptStr2;

        let v: Option<OptStr2> = None;
        assert_eq!(v.as_sea_value(), sea_query::Value::String(None));
    }

    // ---- SqlCondition for basic typed types ----------------------------------

    #[tm_objects::typed(string)]
    struct CondStr;

    #[tm_objects::typed(i64)]
    struct CondI64;

    #[tm_objects::typed(bool)]
    struct CondBool;

    #[ta_objects::Dao]
    #[dao(sql_table = "test_cond")]
    struct CondDao {
        col: CondStr,
        num: CondI64,
        flag: CondBool,
    }

    fn cond_sql(cond: impl crate::sql_condition::SqlCondition) -> String {
        let cond = crate::sql_condition::resolve_condition::<CondDao>(cond).unwrap();
        let q = sea_query::Query::select()
            .column(CondDaoIden::Col)
            .from(CondDaoIden::Table)
            .cond_where(cond)
            .to_string(sea_query::SqliteQueryBuilder);
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
        assert_eq!(cond_sql(&v), r#""num" = 42"#);
    }

    #[test]
    fn test_sql_condition_bool_type() {
        let v = CondBool::from(true);
        assert_eq!(cond_sql(&v), r#""flag" = TRUE"#);
    }
}
