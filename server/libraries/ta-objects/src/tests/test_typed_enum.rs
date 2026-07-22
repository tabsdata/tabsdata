//
// Copyright 2026 Tabsdata Inc.
//

#[cfg(test)]
mod tests {
    use crate as ta_objects;

    use crate::sql_condition::{SqlCondition, resolve_condition};
    use crate::{SeaValue, and, or};
    use sea_query::{Query, SqliteQueryBuilder};

    // ----- Basic typed_enum ---------------------------------------------------

    #[tm_objects::typed_enum]
    #[derive(Eq, PartialEq)]
    enum Color {
        Red,
        Green,
        Blue,
    }

    #[test]
    fn test_display() {
        assert_eq!(Color::Red.to_string(), "Red");
        assert_eq!(Color::Green.to_string(), "Green");
        assert_eq!(Color::Blue.to_string(), "Blue");
    }

    #[test]
    fn test_from_str() {
        assert_eq!("Red".parse::<Color>().unwrap(), Color::Red);
        assert_eq!("Green".parse::<Color>().unwrap(), Color::Green);
        assert_eq!("Blue".parse::<Color>().unwrap(), Color::Blue);
    }

    #[test]
    fn test_from_str_error() {
        let err = "Purple".parse::<Color>();
        assert!(err.is_err());
    }

    #[test]
    fn test_clone_from_ref() {
        let r = &Color::Green;
        let c: Color = r.into();
        assert_eq!(c, Color::Green);
    }

    // ----- Rename attribute ---------------------------------------------------

    #[tm_objects::typed_enum]
    #[derive(Eq, PartialEq)]
    enum Status {
        #[typed_enum(rename = "A")]
        Active,
        #[typed_enum(rename = "F")]
        Frozen,
        #[typed_enum(rename = "D")]
        Deleted,
    }

    #[test]
    fn test_rename_display() {
        assert_eq!(Status::Active.to_string(), "A");
        assert_eq!(Status::Frozen.to_string(), "F");
        assert_eq!(Status::Deleted.to_string(), "D");
    }

    #[test]
    fn test_rename_from_str() {
        assert_eq!("A".parse::<Status>().unwrap(), Status::Active);
        assert_eq!("F".parse::<Status>().unwrap(), Status::Frozen);
        assert_eq!("D".parse::<Status>().unwrap(), Status::Deleted);
    }

    #[test]
    fn test_rename_roundtrip() {
        for s in &["A", "F", "D"] {
            let v: Status = s.parse().unwrap();
            assert_eq!(&v.to_string(), s);
        }
    }

    // ----- Serde --------------------------------------------------------------

    #[test]
    fn test_serde_json() {
        let v = Status::Active;
        let json = serde_json::to_string(&v).unwrap();
        assert_eq!(json, r#""A""#);
        let back: Status = serde_json::from_str(&json).unwrap();
        assert_eq!(back, Status::Active);
    }

    #[test]
    fn test_serde_all_variants() {
        for v in [Status::Active, Status::Frozen, Status::Deleted] {
            let json = serde_json::to_string(&v).unwrap();
            let back: Status = serde_json::from_str(&json).unwrap();
            assert_eq!(back, v);
        }
    }

    // ----- SeaValue -----------------------------------------------------------

    #[test]
    fn test_sea_value() {
        let v = Status::Active;
        assert_eq!(v.as_sea_value(), sea_query::Value::String(Some("A".into())));
    }

    #[test]
    fn test_sea_value_type_id() {
        let v = Status::Active;
        assert_eq!(v.as_type_id(), std::any::TypeId::of::<Status>());
    }

    // ----- SqlCondition -------------------------------------------------------

    #[tm_objects::typed(bool)]
    #[derive(Eq, PartialEq)]
    struct Enabled;

    #[ta_objects::Dao]
    #[dao(sql_table = "test_enum")]
    struct EnumTestDao {
        status: Status,
        enabled: Enabled,
    }

    fn sql(cond: impl SqlCondition) -> String {
        let cond = resolve_condition::<EnumTestDao>(cond).unwrap();
        let q = Query::select()
            .column(EnumTestDaoIden::Status)
            .from(EnumTestDaoIden::Table)
            .cond_where(cond)
            .to_string(SqliteQueryBuilder);
        match q.find("WHERE ") {
            Some(pos) => q[pos + 6..].to_string(),
            None => "TRUE".to_string(),
        }
    }

    #[test]
    fn test_sql_condition_leaf() {
        assert_eq!(sql(&Status::Active), r#""status" = 'A'"#);
        assert_eq!(sql(&Status::Frozen), r#""status" = 'F'"#);
    }

    #[test]
    fn test_sql_condition_any() {
        let cond = or!(Status::Active, Status::Frozen);
        assert_eq!(sql(&cond), r#""status" = 'A' OR "status" = 'F'"#);
    }

    #[test]
    fn test_sql_condition_all_with_other_type() {
        let cond = and!(Status::Active, Enabled::TRUE);
        let s = sql(&cond);
        assert!(s.contains("'A'"), "{s}");
        assert!(s.contains("AND"), "{s}");
    }

    // ----- Case sensitivity ---------------------------------------------------

    #[test]
    fn test_case_sensitive_parsing() {
        // Parsing is case-sensitive: "red" != "Red".
        assert!("red".parse::<Color>().is_err());
        assert!("RED".parse::<Color>().is_err());
        assert!("rEd".parse::<Color>().is_err());
        assert!("Red".parse::<Color>().is_ok());
    }

    #[test]
    fn test_case_sensitive_renamed() {
        // Renamed variants are also case-sensitive.
        assert!("a".parse::<Status>().is_err());
        assert!("active".parse::<Status>().is_err());
        assert!("Active".parse::<Status>().is_err());
        assert!("A".parse::<Status>().is_ok());
    }

    // ----- Whitespace handling ------------------------------------------------

    #[test]
    fn test_whitespace_rejected() {
        assert!(" Red".parse::<Color>().is_err());
        assert!("Red ".parse::<Color>().is_err());
        assert!(" Red ".parse::<Color>().is_err());
        assert!(" A".parse::<Status>().is_err());
        assert!("A ".parse::<Status>().is_err());
    }

    // ----- NullableSeaValue ---------------------------------------------------

    #[test]
    fn test_nullable_sea_value() {
        use crate::NullableSeaValue;
        assert_eq!(Status::null_sea_value(), sea_query::Value::String(None));
    }
}
