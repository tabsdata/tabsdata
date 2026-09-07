//
// Copyright 2026 Tabsdata Inc.
//

#[cfg(test)]
mod tests {
    use crate as ta_objects;
    use tm_objects::{Typed, TypedEnum, TypedOpenApi, TypedSerde, TypedSqlx};

    use crate::and;
    use crate::dxo::{Dao, HasColumn, InsertDao, PkDao, RecursiveDao, TimelineDao, UpdateDao};
    use crate::or;
    use crate::sql_condition::SqlCondition;
    use crate::unique::AddressesOneRow;
    use sea_query::{Iden, Order, PostgresQueryBuilder, Query};
    use std::any::TypeId;
    use td_database::access::{AcrossProjects, DbScope, Global};

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

    #[ta_objects::Dao]
    #[derive(Eq, PartialEq)]
    #[dao(table = "items", ops(select, insert, delete))]
    struct ItemDao {
        pub id: ItemId,
        pub name: Name,
        pub description: Option<Description>,
    }

    #[test]
    fn test_table() {
        let table = ItemDao::TABLE;
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
    fn test_has_column_resolves_own_and_nullable() {
        assert_eq!(<ItemDao as HasColumn<Name>>::COL.quoted(), "name");
        assert_eq!(
            <ItemDao as HasColumn<Description>>::COL.quoted(),
            "description"
        );
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

    #[ta_objects::Dao]
    #[dao(table = "items", ops(update(ItemDao)))]
    struct ItemUpdateDao {
        pub id: ItemId,
        pub name: Name,
        pub description: Option<Description>,
    }

    #[test]
    fn test_update_values() {
        let dao = ItemUpdateDao {
            id: ItemId::try_from("i1").unwrap(),
            name: Name::try_from("n1").unwrap(),
            description: Some(Description::try_from("d1").unwrap()),
        };
        let vals: Vec<_> = dao.update_values().into_iter().collect();
        assert_eq!(vals.len(), 3);
        assert_eq!(vals[0].0.quoted(), "id");
    }

    #[ta_objects::Dao]
    #[dao(table = "ordered_items", ops(select, insert, delete))]
    struct OrderedDao {
        pub id: ItemId,
        #[dao(order_by(asc))]
        pub name: Name,
    }

    #[test]
    fn test_custom_order_by() {
        let order: Vec<_> = OrderedDao::order_by().into_iter().collect();
        assert_eq!(order[0].0.quoted(), "name");
        assert!(matches!(order[0].1, Order::Asc));
    }

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

    #[ta_objects::Dao]
    #[derive(Eq, PartialEq)]
    #[dao(table = "scd2_items", ops(select, insert), scd2)]
    struct Scd2ItemDao {
        #[dao(pk)]
        pub id: ItemId,
        #[dao(partition_by)]
        pub item_id: Name,
        pub status: ItemStatus,
        pub system: System,
        #[dao(valid_from)]
        pub defined_on: DefinedOn,
    }

    impl Scd2ItemDao {
        pub fn active() -> impl SqlCondition<Self> {
            ItemStatus::Active
        }

        pub fn available() -> impl SqlCondition<Self> {
            or!(ItemStatus::Active, ItemStatus::Frozen)
        }

        pub fn executable() -> impl SqlCondition<Self> {
            and!(ItemStatus::Active, System::FALSE)
        }
    }

    #[test]
    fn test_scd2_partition() {
        let col = Scd2ItemDao::PARTITION;
        assert_eq!(col.quoted(), "item_id");
    }

    #[test]
    fn test_scd2_version_order() {
        let col = Scd2ItemDao::VERSION_ORDER;
        assert_eq!(col.quoted(), "defined_on");
    }

    #[test]
    fn test_scd2_is_also_timeline() {
        let col = <Scd2ItemDao as TimelineDao>::VERSION_ORDER;
        assert_eq!(col.quoted(), "defined_on");
    }

    #[ta_objects::Dao]
    #[derive(Eq, PartialEq)]
    #[dao(table = "timeline_items", ops(select, insert), timeline)]
    struct TimelineItemDao {
        #[dao(pk)]
        pub id: ItemId,
        #[dao(partition_by)]
        pub item_id: Name,
        #[dao(version_by)]
        pub defined_on: DefinedOn,
    }

    #[test]
    fn test_timeline_cols() {
        use crate::dxo::TimelineDao;
        assert_eq!(TimelineItemDao::PK.quoted(), "id");
        assert_eq!(TimelineItemDao::PARTITION.quoted(), "item_id");
        assert_eq!(TimelineItemDao::VERSION_ORDER.quoted(), "defined_on");
    }

    #[test]
    fn test_state_all() {
        let cond = SqlCondition::<Scd2ItemDao>::resolve(&());
        let q = Query::select()
            .column(Scd2ItemDaoIden::Id)
            .from(Scd2ItemDaoIden::Table)
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
        let cond = Scd2ItemDao::active().resolve();
        let q = Query::select()
            .column(Scd2ItemDaoIden::Id)
            .from(Scd2ItemDaoIden::Table)
            .cond_where(cond)
            .to_string(PostgresQueryBuilder);
        assert!(q.contains(r#""status" = 'A'"#), "{q}");
        assert!(!q.contains("OR"), "Active is a single condition: {q}");
    }

    #[test]
    fn test_state_available_any() {
        let cond = Scd2ItemDao::available().resolve();
        let q = Query::select()
            .column(Scd2ItemDaoIden::Id)
            .from(Scd2ItemDaoIden::Table)
            .cond_where(cond)
            .to_string(PostgresQueryBuilder);
        assert!(q.contains(r#""status" = 'A'"#), "{q}");
        assert!(q.contains(r#""status" = 'F'"#), "{q}");
        assert!(q.contains("OR"), "Available should be OR: {q}");
    }

    #[test]
    fn test_state_executable_all() {
        let cond = Scd2ItemDao::executable().resolve();
        let q = Query::select()
            .column(Scd2ItemDaoIden::Id)
            .from(Scd2ItemDaoIden::Table)
            .cond_where(cond)
            .to_string(PostgresQueryBuilder);
        assert!(q.contains(r#""status" = 'A'"#), "{q}");
        assert!(q.contains("AND"), "Executable should be AND: {q}");
        assert!(!q.contains("OR"), "Executable should not have OR: {q}");
    }

    #[ta_objects::Dao]
    #[dao(table = "tree", ops(select, insert), scd2, recursive)]
    struct TreeDao {
        #[dao(pk)]
        pub id: ItemId,
        #[dao(partition_by)]
        pub item_id: Name,
        #[dao(recurse_up)]
        pub parent: ParentKey,
        #[dao(recurse_down)]
        pub child: RecursionKey,
        pub status: ItemStatus,
        #[dao(valid_from)]
        pub defined_on: DefinedOn,
    }

    #[test]
    fn test_recursive_up() {
        let col = TreeDao::UP;
        assert_eq!(col.quoted(), "parent");
    }

    #[test]
    fn test_recursive_down() {
        let col = TreeDao::DOWN;
        assert_eq!(col.quoted(), "child");
    }

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
        let dao = ItemUpdateDao {
            id: ItemId::try_from("i1").unwrap(),
            name: Name::try_from("n1").unwrap(),
            description: Some(Description::try_from("d1").unwrap()),
        };
        let cols: Vec<_> = ItemDao::columns().into_iter().collect();
        let vals: Vec<_> = dao.update_values().into_iter().collect();
        assert_eq!(cols.len(), vals.len());
        for (col, (val_col, _)) in cols.iter().zip(vals.iter()) {
            assert_eq!(col.quoted(), val_col.quoted());
        }
    }

    #[test]
    fn test_iden_from_str_case_sensitive() {
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
        let table: ItemDaoIden = "items".parse().unwrap();
        assert_eq!(table.quoted(), "items");
    }

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

    #[test]
    fn test_option_none_skipped_in_update_values() {
        let dao = ItemUpdateDao {
            id: ItemId::try_from("i1").unwrap(),
            name: Name::try_from("n1").unwrap(),
            description: None,
        };
        let sql = Query::update()
            .table(ItemUpdateDaoIden::Table)
            .values(dao.update_values())
            .to_string(PostgresQueryBuilder);
        assert!(
            !sql.contains("description"),
            "None Option field should be skipped in UPDATE: {sql}"
        );
    }

    #[test]
    fn test_option_some_included_in_update_values() {
        let dao = ItemUpdateDao {
            id: ItemId::try_from("i1").unwrap(),
            name: Name::try_from("n1").unwrap(),
            description: Some(Description::try_from("d1").unwrap()),
        };
        let sql = Query::update()
            .table(ItemUpdateDaoIden::Table)
            .values(dao.update_values())
            .to_string(PostgresQueryBuilder);
        assert!(
            sql.contains(r#""description" = 'd1'"#),
            "Some Option field should be included in UPDATE: {sql}"
        );
    }

    #[ta_objects::Dao]
    #[dao(table = "nullable_items", ops(update))]
    struct NullableDao {
        pub id: ItemId,
        pub name: Name,
        pub description: Option<Option<Description>>,
    }

    #[test]
    fn test_double_option_none_skipped_in_update_values() {
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

    #[ta_objects::Dao]
    #[dao(table = "jobs", ops(update(JobDao)))]
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
        assert!(
            sql.contains(r#""name" = 'job1'"#),
            "normal field should be a plain SET: {sql}"
        );
        assert!(
            !sql.contains(r#"CASE WHEN "name""#),
            "normal field should not have CASE WHEN: {sql}"
        );
    }

    #[ta_objects::Dao]
    #[dao(table = "jobs", ops(select, insert))]
    struct JobDao {
        pub id: ItemId,
        pub name: Name,
        pub started_on: Option<StartedOn>,
    }

    #[test]
    fn test_immutable_columns_match_update_values() {
        let dao = ImmutableDao {
            id: ItemId::try_from("j1").unwrap(),
            name: Name::try_from("job1").unwrap(),
            started_on: Some(StartedOn::try_from(100i64).unwrap()),
        };
        let cols: Vec<_> = JobDao::columns().into_iter().collect();
        let vals: Vec<_> = dao.update_values().into_iter().collect();
        assert_eq!(cols.len(), vals.len());
        for (col, (val_col, _)) in cols.iter().zip(vals.iter()) {
            assert_eq!(col.quoted(), val_col.quoted());
        }
    }

    #[ta_objects::Dao]
    #[dao(table = "items", ops(update))]
    struct UpdateItemDB {
        pub name: Option<Name>,
        pub description: Option<Description>,
    }

    #[test]
    fn test_update_dao_has_table() {
        let table = UpdateItemDB::TABLE;
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
        assert_eq!(<UpdateItemDB as HasColumn<Name>>::COL.quoted(), "name");
    }

    #[ta_objects::Dao]
    #[dao(table = "items", ops(select, insert, delete), unique(Name))]
    struct UniqueItemDao {
        #[dao(pk)]
        pub id: ItemId,
        pub name: Name,
    }

    fn addresses_one_row<S: DbScope, C: AddressesOneRow<UniqueItemDao, S>>(_condition: &C) {}

    #[test]
    fn test_struct_level_unique_addresses_one_row() {
        addresses_one_row::<Global, _>(&Name::try_from("n1").unwrap());
        addresses_one_row::<AcrossProjects, _>(&ItemId::try_from("i1").unwrap());
    }

    #[test]
    fn test_bare_update_filters_by_self() {
        assert_eq!(
            TypeId::of::<<UpdateItemDB as UpdateDao>::FilterBy>(),
            TypeId::of::<UpdateItemDB>()
        );
    }

    #[ta_objects::Dao]
    #[dao(table = "items", ops(update(ItemDao)))]
    struct RenameItemDB {
        pub name: Name,
    }

    #[test]
    fn test_update_filters_by_the_named_dao() {
        assert_eq!(
            TypeId::of::<<RenameItemDB as UpdateDao>::FilterBy>(),
            TypeId::of::<ItemDao>()
        );
    }

    #[test]
    fn test_update_dao_condition_uses_filter_by_columns() {
        let cond = SqlCondition::<<RenameItemDB as UpdateDao>::FilterBy>::resolve(
            &ItemId::try_from("i1").unwrap(),
        );
        let sql = Query::update()
            .table(RenameItemDBIden::Table)
            .values(
                RenameItemDB {
                    name: Name::try_from("n2").unwrap(),
                }
                .update_values(),
            )
            .cond_where(cond)
            .to_string(PostgresQueryBuilder);
        assert!(sql.contains(r#""id" = 'i1'"#), "{sql}");
    }
}
