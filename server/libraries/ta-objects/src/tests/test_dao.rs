//
// Copyright 2026 Tabsdata Inc.
//

/// Tests for the `#[Dao]` proc macro: verifies that `DaoCore`, `Dao`,
/// `VersionedDao` and `Recursive` trait impls and state-filter methods are generated
/// correctly.
#[cfg(test)]
mod tests {
    use crate as ta_objects;
    use crate::dxo::StateFilter;
    use tm_objects::{Typed, TypedEnum, TypedOpenApi, TypedSerde, TypedSqlx};

    use crate::and;
    use crate::dxo::{Dao, DaoCore, DaoUpdate, Recursive, TypedColumns, VersionedDao};
    use crate::or;
    use crate::sql_condition::SqlCondition;
    use sea_query::{Iden, Order, PostgresQueryBuilder, Query};
    use std::any::TypeId;

    // ---- Test types ----------------------------------------------------------

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct Name(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct Description(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct DefinedOn(i64);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct ItemId(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct RecursionKey(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct ParentKey(String);

    #[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    enum ItemStatus {
        #[typed_enum(rename = "A")]
        Active,
        #[typed_enum(rename = "F")]
        Frozen,
        #[typed_enum(rename = "D")]
        Deleted,
    }

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct System(bool);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct StartedOn(i64);

    // ---- Plain Dao -----------------------------------------------------------

    #[ta_objects::Dao]
    #[derive(Eq, PartialEq)]
    #[dao(sql_table = "items")]
    struct ItemDao {
        pub id: ItemId,
        pub name: Name,
        pub description: Option<Description>,
    }

    #[test]
    fn test_table() {
        let table = ItemDao::table();
        assert_eq!(table.quoted(), "items");
    }

    #[test]
    fn test_columns() {
        let cols: Vec<_> = ItemDao::columns().into_iter().collect();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].quoted(), "id");
        assert_eq!(cols[1].quoted(), "name");
        assert_eq!(cols[2].quoted(), "description");
    }

    #[test]
    fn test_order_by_default_first_column_desc() {
        let order: Vec<_> = ItemDao::order_by().into_iter().collect();
        assert_eq!(order.len(), 1);
        assert_eq!(order[0].0.quoted(), "id");
        assert!(matches!(order[0].1, Order::Desc));
    }

    #[test]
    fn test_col_for_type() {
        let col = ItemDao::col_for_type(TypeId::of::<Name>());
        assert!(col.is_some());
        assert_eq!(col.unwrap().quoted(), "name");

        let col = ItemDao::col_for_type(TypeId::of::<Option<Description>>());
        assert!(col.is_some());

        // DefinedOn is not a field in ItemDao.
        let col = ItemDao::col_for_type(TypeId::of::<DefinedOn>());
        assert!(col.is_none());
    }

    #[test]
    fn test_insert_values() {
        let dao = ItemDao {
            id: ItemId::try_from("i1").unwrap(),
            name: Name::try_from("n1").unwrap(),
            description: Some(Description::try_from("d1").unwrap()),
        };
        let vals: Vec<_> = dao.insert_values().into_iter().collect();
        assert_eq!(vals.len(), 3);
    }

    #[test]
    fn test_update_values() {
        let dao = ItemDao {
            id: ItemId::try_from("i1").unwrap(),
            name: Name::try_from("n1").unwrap(),
            description: Some(Description::try_from("d1").unwrap()),
        };
        let vals: Vec<_> = dao.update_values().into_iter().collect();
        assert_eq!(vals.len(), 3);
        assert_eq!(vals[0].0.quoted(), "id");
    }

    // ---- Dao with order_by ---------------------------------------------------

    #[ta_objects::Dao]
    #[dao(sql_table = "ordered_items", order_by(col = "name", dir = "asc"))]
    struct OrderedDao {
        pub id: ItemId,
        pub name: Name,
    }

    #[test]
    fn test_custom_order_by() {
        let order: Vec<_> = OrderedDao::order_by().into_iter().collect();
        assert_eq!(order[0].0.quoted(), "name");
        assert!(matches!(order[0].1, Order::Asc));
    }

    // ---- SqlCondition::resolve ------------------------------------------------

    #[test]
    fn test_resolve_condition_leaf() {
        let name = Name::try_from("hello").unwrap();
        let cond = SqlCondition::<ItemDao>::resolve(&name);
        let q = Query::select()
            .column(ItemDaoIden::Id)
            .from(ItemDaoIden::Table)
            .cond_where(cond)
            .to_string(PostgresQueryBuilder);
        assert!(q.contains(r#""name" = 'hello'"#), "{q}");
    }

    #[test]
    fn test_resolve_condition_unit() {
        let cond = SqlCondition::<ItemDao>::resolve(&());
        let q = Query::select()
            .column(ItemDaoIden::Id)
            .from(ItemDaoIden::Table)
            .cond_where(cond)
            .to_string(PostgresQueryBuilder);
        // Empty condition renders as WHERE TRUE (no actual filter).
        assert!(
            !q.contains("WHERE") || q.contains("WHERE TRUE"),
            "unit should produce no real filter: {q}"
        );
    }

    // ---- Iden FromStr --------------------------------------------------------

    #[test]
    fn test_iden_from_str() {
        let col: ItemDaoIden = "id".parse().unwrap();
        assert_eq!(col.quoted(), "id");

        let col: ItemDaoIden = "name".parse().unwrap();
        assert_eq!(col.quoted(), "name");
    }

    #[test]
    fn test_iden_from_str_unknown() {
        let result = "nonexistent".parse::<ItemDaoIden>();
        assert!(result.is_err());
    }

    // ---- Versioned Dao with States -------------------------------------------

    #[ta_objects::Dao]
    #[derive(Eq, PartialEq)]
    #[dao(
        sql_table = "versioned_items",
        versioned(pk = "id", version_by = "defined_on", partition_by = "item_id")
    )]
    struct VersionedItemDao {
        pub id: ItemId,
        pub item_id: Name,
        pub status: ItemStatus,
        pub system: System,
        pub defined_on: DefinedOn,
    }

    impl VersionedItemDao {
        pub fn active() -> StateFilter<Self> {
            StateFilter::of(ItemStatus::Active)
        }

        pub fn available() -> StateFilter<Self> {
            StateFilter::of(or!(ItemStatus::Active, ItemStatus::Frozen))
        }

        pub fn executable() -> StateFilter<Self> {
            StateFilter::of(and!(ItemStatus::Active, System::FALSE))
        }
    }

    #[test]
    fn test_versioned_partition() {
        let col = VersionedItemDao::partition_col();
        assert_eq!(col.quoted(), "item_id");
    }

    #[test]
    fn test_versioned_version_order() {
        let col = VersionedItemDao::version_order_col();
        assert_eq!(col.quoted(), "defined_on");
    }

    #[test]
    fn test_state_all() {
        // S=0 is always the empty condition.
        let cond = StateFilter::<VersionedItemDao>::all().condition().clone();
        let q = Query::select()
            .column(VersionedItemDaoIden::Id)
            .from(VersionedItemDaoIden::Table)
            .cond_where(cond)
            .to_string(PostgresQueryBuilder);
        // Empty condition renders as WHERE TRUE (no actual filter).
        assert!(
            !q.contains("WHERE") || q.contains("WHERE TRUE"),
            "All state should produce no real filter: {q}"
        );
    }

    #[test]
    fn test_state_active() {
        let cond = VersionedItemDao::active().condition().clone();
        let q = Query::select()
            .column(VersionedItemDaoIden::Id)
            .from(VersionedItemDaoIden::Table)
            .cond_where(cond)
            .to_string(PostgresQueryBuilder);
        assert!(q.contains(r#""status" = 'A'"#), "{q}");
        assert!(!q.contains("OR"), "Active is a single condition: {q}");
    }

    #[test]
    fn test_state_available_any() {
        let cond = VersionedItemDao::available().condition().clone();
        let q = Query::select()
            .column(VersionedItemDaoIden::Id)
            .from(VersionedItemDaoIden::Table)
            .cond_where(cond)
            .to_string(PostgresQueryBuilder);
        assert!(q.contains(r#""status" = 'A'"#), "{q}");
        assert!(q.contains(r#""status" = 'F'"#), "{q}");
        assert!(q.contains("OR"), "Available should be OR: {q}");
    }

    #[test]
    fn test_state_executable_all() {
        let cond = VersionedItemDao::executable().condition().clone();
        let q = Query::select()
            .column(VersionedItemDaoIden::Id)
            .from(VersionedItemDaoIden::Table)
            .cond_where(cond)
            .to_string(PostgresQueryBuilder);
        assert!(q.contains(r#""status" = 'A'"#), "{q}");
        assert!(q.contains("AND"), "Executable should be AND: {q}");
        assert!(!q.contains("OR"), "Executable should not have OR: {q}");
    }

    // ---- Recursive Dao -------------------------------------------------------

    #[ta_objects::Dao]
    #[dao(
        sql_table = "tree",
        versioned(pk = "id", version_by = "defined_on", partition_by = "item_id"),
        recursive(up = "parent", down = "child")
    )]
    struct TreeDao {
        pub id: ItemId,
        pub item_id: Name,
        pub parent: ParentKey,
        pub child: RecursionKey,
        pub status: ItemStatus,
        pub defined_on: DefinedOn,
    }

    #[test]
    fn test_recursive_up() {
        let col = TreeDao::recurse_up();
        assert_eq!(col.quoted(), "parent");
    }

    #[test]
    fn test_recursive_down() {
        let col = TreeDao::recurse_down();
        assert_eq!(col.quoted(), "child");
    }

    // ---- Column / value ordering consistency ---------------------------------

    #[test]
    fn test_insert_values_match_columns() {
        let dao = ItemDao {
            id: ItemId::try_from("i1").unwrap(),
            name: Name::try_from("n1").unwrap(),
            description: Some(Description::try_from("d1").unwrap()),
        };
        let cols: Vec<_> = ItemDao::columns().into_iter().collect();
        let vals: Vec<_> = dao.insert_values().into_iter().collect();
        assert_eq!(cols.len(), vals.len());
    }

    #[test]
    fn test_update_values_match_columns() {
        let dao = ItemDao {
            id: ItemId::try_from("i1").unwrap(),
            name: Name::try_from("n1").unwrap(),
            description: Some(Description::try_from("d1").unwrap()),
        };
        let cols: Vec<_> = ItemDao::columns().into_iter().collect();
        let vals: Vec<_> = dao.update_values().into_iter().collect();
        assert_eq!(cols.len(), vals.len());
        // Each update_values entry column matches the columns() order.
        for (col, (val_col, _)) in cols.iter().zip(vals.iter()) {
            assert_eq!(col.quoted(), val_col.quoted());
        }
    }

    // ---- Iden edge cases -----------------------------------------------------

    #[test]
    fn test_iden_from_str_case_sensitive() {
        // Column names are case-sensitive.
        assert!("Id".parse::<ItemDaoIden>().is_err());
        assert!("ID".parse::<ItemDaoIden>().is_err());
        assert!("NAME".parse::<ItemDaoIden>().is_err());
    }

    #[test]
    fn test_iden_from_str_whitespace() {
        assert!(" id".parse::<ItemDaoIden>().is_err());
        assert!("id ".parse::<ItemDaoIden>().is_err());
        assert!(" id ".parse::<ItemDaoIden>().is_err());
    }

    #[test]
    fn test_iden_from_str_empty() {
        assert!("".parse::<ItemDaoIden>().is_err());
    }

    #[test]
    fn test_iden_table_variant() {
        // The Table variant can be parsed from the table name.
        let table: ItemDaoIden = "items".parse().unwrap();
        assert_eq!(table.quoted(), "items");
    }

    // ---- Option field in Dao -------------------------------------------------

    #[test]
    fn test_option_field_explicit_none() {
        let dao = ItemDao {
            id: ItemId::try_from("i1").unwrap(),
            name: Name::try_from("n1").unwrap(),
            description: None,
        };
        assert_eq!(dao.description, None);
    }

    #[test]
    fn test_option_field_some() {
        let dao = ItemDao {
            id: ItemId::try_from("i1").unwrap(),
            name: Name::try_from("n1").unwrap(),
            description: Some(Description::try_from("d1").unwrap()),
        };
        assert_eq!(dao.description, Some(Description::try_from("d1").unwrap()));
    }

    // ---- Option fields skip None in update_values ----------------------------

    #[test]
    fn test_option_none_skipped_in_update_values() {
        let dao = ItemDao {
            id: ItemId::try_from("i1").unwrap(),
            name: Name::try_from("n1").unwrap(),
            description: None,
        };
        let sql = Query::update()
            .table(ItemDaoIden::Table)
            .values(dao.update_values())
            .to_string(PostgresQueryBuilder);
        assert!(
            !sql.contains("description"),
            "None Option field should be skipped in UPDATE: {sql}"
        );
    }

    #[test]
    fn test_option_some_included_in_update_values() {
        let dao = ItemDao {
            id: ItemId::try_from("i1").unwrap(),
            name: Name::try_from("n1").unwrap(),
            description: Some(Description::try_from("d1").unwrap()),
        };
        let sql = Query::update()
            .table(ItemDaoIden::Table)
            .values(dao.update_values())
            .to_string(PostgresQueryBuilder);
        assert!(
            sql.contains(r#""description" = 'd1'"#),
            "Some Option field should be included in UPDATE: {sql}"
        );
    }

    // ---- Option<Option<T>> tri-state in update_values -------------------------

    #[ta_objects::Dao]
    #[dao(sql_table = "nullable_items")]
    struct NullableDao {
        pub id: ItemId,
        pub name: Name,
        pub description: Option<Option<Description>>,
    }

    #[test]
    fn test_double_option_none_skipped_in_update_values() {
        // None (outer) → column should be skipped entirely.
        let dao = NullableDao {
            id: ItemId::try_from("i1").unwrap(),
            name: Name::try_from("n1").unwrap(),
            description: None,
        };
        let sql = Query::update()
            .table(NullableDaoIden::Table)
            .values(dao.update_values())
            .to_string(PostgresQueryBuilder);
        assert!(
            !sql.contains("description"),
            "None (outer) should skip column in UPDATE: {sql}"
        );
    }

    #[test]
    fn test_double_option_some_none_sets_null_in_update_values() {
        // Some(None) → column should be SET to NULL.
        let dao = NullableDao {
            id: ItemId::try_from("i1").unwrap(),
            name: Name::try_from("n1").unwrap(),
            description: Some(None),
        };
        let sql = Query::update()
            .table(NullableDaoIden::Table)
            .values(dao.update_values())
            .to_string(PostgresQueryBuilder);
        assert!(
            sql.contains(r#""description" = NULL"#),
            "Some(None) should SET column to NULL: {sql}"
        );
    }

    #[test]
    fn test_double_option_some_some_sets_value_in_update_values() {
        // Some(Some(v)) → column should be SET to the value.
        let dao = NullableDao {
            id: ItemId::try_from("i1").unwrap(),
            name: Name::try_from("n1").unwrap(),
            description: Some(Some(Description::try_from("d1").unwrap())),
        };
        let sql = Query::update()
            .table(NullableDaoIden::Table)
            .values(dao.update_values())
            .to_string(PostgresQueryBuilder);
        assert!(
            sql.contains(r#""description" = 'd1'"#),
            "Some(Some(v)) should SET column to value: {sql}"
        );
    }

    // ---- Immutable fields ----------------------------------------------------

    #[ta_objects::Dao]
    #[dao(sql_table = "jobs")]
    struct ImmutableDao {
        pub id: ItemId,
        pub name: Name,
        #[dao(immutable)]
        pub started_on: Option<StartedOn>,
    }

    #[test]
    fn test_immutable_update_generates_case_when() {
        let dao = ImmutableDao {
            id: ItemId::try_from("j1").unwrap(),
            name: Name::try_from("job1").unwrap(),
            started_on: Some(StartedOn::try_from(100i64).unwrap()),
        };
        let sql = Query::update()
            .table(ImmutableDaoIden::Table)
            .values(dao.update_values())
            .to_string(PostgresQueryBuilder);
        // Immutable field should use CASE WHEN ... IS NULL THEN ... ELSE ... END.
        assert!(
            sql.contains(r#"CASE WHEN ("started_on" IS NULL) THEN"#),
            "immutable field should produce CASE WHEN: {sql}"
        );
        assert!(
            sql.contains(r#"ELSE "started_on" END)"#),
            "immutable field should keep existing value in ELSE: {sql}"
        );
    }

    #[test]
    fn test_immutable_normal_fields_are_plain() {
        let dao = ImmutableDao {
            id: ItemId::try_from("j1").unwrap(),
            name: Name::try_from("job1").unwrap(),
            started_on: Some(StartedOn::try_from(100i64).unwrap()),
        };
        let sql = Query::update()
            .table(ImmutableDaoIden::Table)
            .values(dao.update_values())
            .to_string(PostgresQueryBuilder);
        // Normal fields should NOT have CASE WHEN.
        assert!(
            sql.contains(r#""name" = 'job1'"#),
            "normal field should be a plain SET: {sql}"
        );
        assert!(
            !sql.contains(r#"CASE WHEN "name""#),
            "normal field should not have CASE WHEN: {sql}"
        );
    }

    #[test]
    fn test_immutable_columns_match_update_values() {
        let dao = ImmutableDao {
            id: ItemId::try_from("j1").unwrap(),
            name: Name::try_from("job1").unwrap(),
            started_on: Some(StartedOn::try_from(100i64).unwrap()),
        };
        let cols: Vec<_> = ImmutableDao::columns().into_iter().collect();
        let vals: Vec<_> = dao.update_values().into_iter().collect();
        assert_eq!(cols.len(), vals.len());
        for (col, (val_col, _)) in cols.iter().zip(vals.iter()) {
            assert_eq!(col.quoted(), val_col.quoted());
        }
    }

    // ---- Update-only Dao (#[dao(update)]) ------------------------------------

    #[ta_objects::Dao]
    #[dao(update, sql_table = "items")]
    struct UpdateItemDB {
        pub name: Option<Name>,
        pub description: Option<Description>,
    }

    #[test]
    fn test_update_dao_has_table() {
        let table = UpdateItemDB::table();
        assert_eq!(table.quoted(), "items");
    }

    #[test]
    fn test_update_dao_has_display_name() {
        assert_eq!(UpdateItemDB::display_name(), "Update Item");
    }

    #[test]
    fn test_update_dao_update_values() {
        let dao = UpdateItemDB {
            name: Some(Name::try_from("n1").unwrap()),
            description: None,
        };
        let sql = Query::update()
            .table(UpdateItemDBIden::Table)
            .values(dao.update_values())
            .to_string(PostgresQueryBuilder);
        assert!(
            sql.contains(r#""name" = 'n1'"#),
            "update dao should produce SET: {sql}"
        );
        assert!(
            !sql.contains("description"),
            "None fields should be skipped: {sql}"
        );
    }

    #[test]
    fn test_update_dao_has_typed_columns() {
        let col = UpdateItemDB::col_for_type(TypeId::of::<Option<Name>>());
        assert!(col.is_some());
        assert_eq!(col.unwrap().quoted(), "name");
    }
}
