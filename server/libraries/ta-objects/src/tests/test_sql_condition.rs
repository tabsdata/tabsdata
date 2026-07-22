//
// Copyright 2026 Tabsdata Inc.
//

#[cfg(test)]
mod tests {
    use crate as ta_objects;

    use crate::And;
    use crate::Or;
    use crate::and;
    use crate::or;
    use crate::sql_condition::{SqlCondition, resolve_condition};
    use sea_query::{Query, SqliteQueryBuilder};

    #[tm_objects::typed(string)]
    struct VCol;

    /// Dummy DAO that maps every type to column `v`.
    #[ta_objects::Dao]
    #[dao(sql_table = "test_table")]
    struct TestDao {
        v: VCol,
    }

    /// Render a `SqlCondition` to the WHERE clause string (SQLite dialect).
    /// Returns `"TRUE"` when the condition is empty.
    fn sql(cond: impl SqlCondition) -> String {
        let cond = resolve_condition::<TestDao>(cond).unwrap();
        let q = Query::select()
            .column(TestDaoIden::V)
            .from(TestDaoIden::V)
            .cond_where(cond)
            .to_string(SqliteQueryBuilder);
        match q.find("WHERE ") {
            Some(pos) => q[pos + 6..].to_string(),
            None => "TRUE".to_string(),
        }
    }

    // ----- unit (empty) -------------------------------------------------------

    #[test]
    fn test_unit() {
        assert_eq!(sql(()), "TRUE");
        assert_eq!(sql(()), "TRUE");
    }

    // ----- leaf types ---------------------------------------------------------

    #[test]
    fn test_leaf() {
        let v = VCol::try_from("my_coll").unwrap();
        assert_eq!(sql(&v), r#""v" = 'my_coll'"#);
    }

    // ----- &T delegation ------------------------------------------------------

    #[test]
    fn test_ref_delegation() {
        let v = VCol::try_from("ref_coll").unwrap();
        assert_eq!(sql(&v), r#""v" = 'ref_coll'"#);
    }

    // ----- and! ---------------------------------------------------------------

    #[test]
    fn test_all1() {
        let v = VCol::try_from("a").unwrap();
        assert_eq!(sql(and!(v)), r#""v" = 'a'"#);
    }

    #[test]
    fn test_all2() {
        let a = VCol::try_from("c").unwrap();
        let b = VCol::try_from("t").unwrap();
        assert_eq!(sql(and!(a, b)), r#""v" = 'c' AND "v" = 't'"#);
    }

    #[test]
    fn test_all3() {
        let a = VCol::try_from("c").unwrap();
        let b = VCol::try_from("t").unwrap();
        let c = VCol::try_from("col").unwrap();
        assert_eq!(
            sql(and!(a, b, c)),
            r#""v" = 'c' AND "v" = 't' AND "v" = 'col'"#
        );
    }

    #[test]
    fn test_all4() {
        let a = VCol::try_from("c1").unwrap();
        let b = VCol::try_from("c2").unwrap();
        let c = VCol::try_from("t1").unwrap();
        let d = VCol::try_from("t2").unwrap();
        assert_eq!(
            sql(and!(a, b, c, d)),
            r#""v" = 'c1' AND "v" = 'c2' AND "v" = 't1' AND "v" = 't2'"#
        );
    }

    // ----- or! ---------------------------------------------------------------

    #[test]
    fn test_any1() {
        let v = VCol::try_from("b").unwrap();
        assert_eq!(sql(or!(v)), r#""v" = 'b'"#);
    }

    #[test]
    fn test_any2() {
        let a = VCol::try_from("c1").unwrap();
        let b = VCol::try_from("c2").unwrap();
        assert_eq!(sql(or!(a, b)), r#""v" = 'c1' OR "v" = 'c2'"#);
    }

    #[test]
    fn test_any3() {
        let a = VCol::try_from("c1").unwrap();
        let b = VCol::try_from("c2").unwrap();
        let c = VCol::try_from("c3").unwrap();
        assert_eq!(
            sql(or!(a, b, c)),
            r#""v" = 'c1' OR "v" = 'c2' OR "v" = 'c3'"#
        );
    }

    #[test]
    fn test_any4() {
        let a = VCol::try_from("c1").unwrap();
        let b = VCol::try_from("c2").unwrap();
        let c = VCol::try_from("c3").unwrap();
        let d = VCol::try_from("c4").unwrap();
        assert_eq!(
            sql(or!(a, b, c, d)),
            r#""v" = 'c1' OR "v" = 'c2' OR "v" = 'c3' OR "v" = 'c4'"#
        );
    }

    // ----- Nesting ------------------------------------------------------------

    #[test]
    fn test_all_containing_any() {
        let a = VCol::try_from("c").unwrap();
        let b = VCol::try_from("t1").unwrap();
        let c = VCol::try_from("t2").unwrap();
        assert_eq!(
            sql(and!(a, or!(b, c))),
            r#""v" = 'c' AND ("v" = 't1' OR "v" = 't2')"#
        );
    }

    #[test]
    fn test_any_of_alls() {
        let a = VCol::try_from("c1").unwrap();
        let b = VCol::try_from("t1").unwrap();
        let c = VCol::try_from("c2").unwrap();
        let d = VCol::try_from("t2").unwrap();
        assert_eq!(
            sql(or!(and!(a, b), and!(c, d))),
            r#"("v" = 'c1' AND "v" = 't1') OR ("v" = 'c2' AND "v" = 't2')"#
        );
    }

    #[test]
    fn test_all_of_anys() {
        let a = VCol::try_from("c1").unwrap();
        let b = VCol::try_from("c2").unwrap();
        let c = VCol::try_from("t1").unwrap();
        let d = VCol::try_from("t2").unwrap();
        assert_eq!(
            sql(and!(or!(a, b), or!(c, d))),
            r#"("v" = 'c1' OR "v" = 'c2') AND ("v" = 't1' OR "v" = 't2')"#
        );
    }

    // ----- Type-position macros -----------------------------------------------

    #[test]
    fn test_type_macros_compile() {
        fn _all1(_: And![VCol]) {}
        fn _all2(_: And![VCol, VCol]) {}
        fn _all3(_: And![VCol, VCol, VCol]) {}
        fn _all4(_: And![VCol, VCol, VCol, VCol]) {}
        fn _any1(_: Or![VCol]) {}
        fn _any2(_: Or![VCol, VCol]) {}
        fn _any3(_: Or![VCol, VCol, VCol]) {}
        fn _any4(_: Or![VCol, VCol, VCol, VCol]) {}
    }

    // ----- Trailing commas ----------------------------------------------------

    #[test]
    fn test_trailing_commas() {
        let a = VCol::try_from("a").unwrap();
        let b = VCol::try_from("b").unwrap();

        // Value-position macros accept trailing commas.
        let _ = and!(a.clone(),);
        let _ = and!(a.clone(), b.clone(),);
        let _ = or!(a.clone(),);
        let _ = or!(a.clone(), b.clone(),);

        // Type-position macros accept trailing commas.
        fn _t1(_: And![VCol,]) {}
        fn _t2(_: And![VCol, VCol,]) {}
        fn _t3(_: Or![VCol,]) {}
        fn _t4(_: Or![VCol, VCol,]) {}
    }

    // ----- Error propagation --------------------------------------------------

    #[tm_objects::typed(string)]
    struct _UnmappedCol;

    /// A DAO with no type mappings — resolving any typed value should fail.
    #[ta_objects::Dao]
    #[dao(sql_table = "unmapped")]
    struct UnmappedDao {
        dummy: _UnmappedCol,
    }

    #[test]
    fn test_resolver_error_propagates_leaf() {
        let v = VCol::try_from("x").unwrap();
        // Use a DAO that doesn't map VCol → should fail
        assert!(resolve_condition::<UnmappedDao>(&v).is_err());
    }

    #[test]
    fn test_resolver_error_propagates_all() {
        let a = VCol::try_from("a").unwrap();
        let b = VCol::try_from("b").unwrap();
        let cond = and!(a, b);
        assert!(resolve_condition::<UnmappedDao>(&cond).is_err());
    }

    #[test]
    fn test_resolver_error_propagates_any() {
        let a = VCol::try_from("a").unwrap();
        let b = VCol::try_from("b").unwrap();
        let cond = or!(a, b);
        assert!(resolve_condition::<UnmappedDao>(&cond).is_err());
    }

    #[test]
    fn test_resolver_error_propagates_nested() {
        let a = VCol::try_from("a").unwrap();
        let b = VCol::try_from("b").unwrap();
        let c = VCol::try_from("c").unwrap();
        let cond = and!(a, or!(b, c));
        assert!(resolve_condition::<UnmappedDao>(&cond).is_err());
    }
}
