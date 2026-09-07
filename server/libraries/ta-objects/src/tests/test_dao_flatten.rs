//
// Copyright 2026 Tabsdata Inc.
//

#[cfg(test)]
mod tests {
    use crate as ta_objects;
    use tm_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};

    use crate::dxo::{
        Dao, Insertable, Partitioned, PkDao, RecursiveDao, Scd2Dao, SelectDao, TimelineDao,
        UpdateDao, UpsertDao,
    };
    use crate::unique::AddressesOneRow;
    use sea_query::{Iden, Order};
    use std::str::FromStr;
    use td_database::access::{DbScope, Global};

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct WidgetId(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct Name(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    #[typed(sealed)]
    struct DefinedOn(i64);

    impl DefinedOn {
        fn at(v: i64) -> Self {
            Self(v)
        }
    }

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct ValidTo(i64);

    #[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
    struct Scd2Interval {
        pub valid_from: DefinedOn,
        pub valid_to: ValidTo,
    }

    impl crate::IntervalColumns for Scd2Interval {
        type ValidFrom = DefinedOn;
        type ValidTo = ValidTo;
    }

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct OwnerName(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct RenamedName(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct WidgetVersionId(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct WidgetRowId(String);

    #[ta_objects::Dao]
    #[derive(Eq, PartialEq)]
    #[dao(table = "widgets", ops(select, insert, delete), unique(Name))]
    struct WidgetDao {
        #[dao(pk)]
        pub id: WidgetId,
        pub name: Name,
        pub defined_on: Option<DefinedOn>,
    }

    #[ta_objects::Dao]
    #[derive(Eq, PartialEq)]
    #[dao(table = "widgets__with_names", ops(select, insert, delete))]
    struct WidgetDaoWithNames {
        #[dao(flatten)]
        pub base: WidgetDao,
        pub owner: OwnerName,
        pub renamed: RenamedName,
    }

    #[ta_objects::Dao]
    #[derive(Eq, PartialEq)]
    #[dao(table = "widgets__with_status", ops(select, insert, delete))]
    struct WidgetDaoWithStatus {
        #[dao(flatten)]
        pub base: WidgetDaoWithNames,
        pub started_on: DefinedOn,
    }

    fn col_names<D: SelectDao>() -> Vec<String> {
        D::columns()
            .into_iter()
            .map(|c| Iden::to_string(&c))
            .collect()
    }

    #[test]
    fn test_table_and_column_rendering() {
        assert_eq!(
            Iden::to_string(&WidgetDaoWithNames::TABLE),
            "widgets__with_names"
        );
        assert_eq!(
            col_names::<WidgetDaoWithNames>(),
            ["id", "name", "defined_on", "owner", "renamed"]
        );
    }

    #[test]
    fn test_chained_flatten_columns() {
        assert_eq!(
            col_names::<WidgetDaoWithStatus>(),
            ["id", "name", "defined_on", "owner", "renamed", "started_on"]
        );
    }

    #[test]
    fn test_from_str_resolves_base_and_own() {
        let own = WidgetDaoWithNamesIden::from_str("owner").unwrap();
        assert_eq!(own, WidgetDaoWithNamesIden::Owner);

        let base = WidgetDaoWithNamesIden::from_str("name").unwrap();
        assert_eq!(base, WidgetDaoWithNamesIden::Base(WidgetDaoIden::Name));

        assert!(WidgetDaoWithNamesIden::from_str("nonexistent").is_err());

        let deep = WidgetDaoWithStatusIden::from_str("id").unwrap();
        assert_eq!(Iden::to_string(&deep), "id");
    }

    #[test]
    fn test_has_column_resolves_base_and_own() {
        use crate::dxo::HasColumn;
        assert_eq!(
            <WidgetDaoWithNames as HasColumn<Name>>::COL,
            WidgetDaoWithNamesIden::Base(WidgetDaoIden::Name)
        );
        assert_eq!(
            <WidgetDaoWithNames as HasColumn<OwnerName>>::COL,
            WidgetDaoWithNamesIden::Owner
        );
        assert_eq!(
            Iden::to_string(&<WidgetDaoWithStatus as HasColumn<Name>>::COL),
            "name"
        );
    }

    #[test]
    fn test_insert_values_match_columns() {
        let dao = WidgetDaoWithNames {
            base: WidgetDao {
                id: WidgetId::try_from("w1").unwrap(),
                name: Name::try_from("n").unwrap(),
                defined_on: Some(DefinedOn::at(1)),
            },
            owner: OwnerName::try_from("o").unwrap(),
            renamed: RenamedName::try_from("r").unwrap(),
        };
        let cols = WidgetDaoWithNames::columns().into_iter().count();
        let vals = dao.insert_values().into_iter().count();
        assert_eq!(cols, vals);
        assert_eq!(dao.column_display_values().len(), cols);
    }

    #[test]
    fn test_default_order_by_delegates_to_base() {
        let order: Vec<_> = WidgetDaoWithNames::order_by().into_iter().collect();
        assert_eq!(order.len(), 1);
        assert_eq!(Iden::to_string(&order[0].0), "id");
        assert!(matches!(order[0].1, Order::Desc));
    }

    #[ta_objects::Dao]
    #[dao(table = "widgets", ops(update(WidgetDao)))]
    struct WidgetUpdate {
        pub id: WidgetId,
        pub name: Name,
        pub defined_on: Option<DefinedOn>,
    }

    #[ta_objects::Dao]
    #[dao(table = "widgets", ops(update(WidgetDao)))]
    struct WidgetUpdateWithNames {
        #[dao(flatten)]
        pub base: WidgetUpdate,
        pub owner: OwnerName,
        pub renamed: RenamedName,
    }

    #[test]
    fn test_update_values_delegate_base() {
        let dao = WidgetUpdateWithNames {
            base: WidgetUpdate {
                id: WidgetId::try_from("w1").unwrap(),
                name: Name::try_from("n").unwrap(),
                defined_on: None,
            },
            owner: OwnerName::try_from("o").unwrap(),
            renamed: RenamedName::try_from("r").unwrap(),
        };
        let cols: Vec<String> = dao
            .update_values()
            .into_iter()
            .map(|(c, _)| Iden::to_string(&c))
            .collect();
        assert_eq!(cols, ["id", "name", "owner", "renamed"]);
    }

    #[ta_objects::Dao]
    #[derive(Eq, PartialEq)]
    #[dao(table = "scd2_widgets", ops(insert), scd2, recursive)]
    struct Scd2WidgetInsertDao {
        #[dao(pk, recursive(down))]
        pub id: WidgetVersionId,
        #[dao(partition_by, recursive(up))]
        pub widget_id: WidgetId,
    }

    #[ta_objects::Dao]
    #[derive(Eq, PartialEq)]
    #[dao(table = "scd2_widgets", ops(select), scd2, recursive)]
    struct Scd2WidgetDao {
        #[dao(flatten)]
        pub base: Scd2WidgetInsertDao,
        #[dao(flatten)]
        pub interval: Scd2Interval,
    }

    #[ta_objects::Dao]
    #[derive(Eq, PartialEq)]
    #[dao(view = "scd2_widgets__with_names", scd2, recursive)]
    struct Scd2WidgetDaoWithNames {
        #[dao(flatten)]
        pub base: Scd2WidgetDao,
        pub owner: OwnerName,
    }

    #[test]
    fn test_scd2_cols_resolve_into_base() {
        assert_eq!(Iden::to_string(&Scd2WidgetDaoWithNames::PK), "id");
        assert_eq!(
            Iden::to_string(&Scd2WidgetDaoWithNames::PARTITION),
            "widget_id"
        );
        assert_eq!(
            Iden::to_string(&Scd2WidgetDaoWithNames::VERSION_ORDER),
            "valid_from"
        );
        assert_eq!(
            Iden::to_string(&Scd2WidgetDaoWithNames::VALID_TO),
            "valid_to"
        );
    }

    #[test]
    fn test_recursive_cols_resolve_into_base() {
        assert_eq!(Iden::to_string(&Scd2WidgetDaoWithNames::UP), "widget_id");
        assert_eq!(Iden::to_string(&Scd2WidgetDaoWithNames::DOWN), "id");
    }

    #[ta_objects::Dao]
    #[derive(Eq, PartialEq)]
    #[dao(table = "widgets_rows", ops(select, insert, delete, upsert(Name)))]
    struct WidgetRowDao {
        #[dao(flatten)]
        pub base: WidgetDao,
        pub row_id: WidgetRowId,
    }

    #[test]
    fn test_keyed_cols_resolve_into_base() {
        let keys: Vec<String> = WidgetRowDao::key_columns()
            .into_iter()
            .map(|c| Iden::to_string(&c))
            .collect();
        assert_eq!(keys, ["name"]);

        let values: Vec<String> = WidgetRowDao::value_columns()
            .into_iter()
            .map(|c| Iden::to_string(&c))
            .collect();
        assert_eq!(values, ["id", "defined_on", "row_id"]);
    }

    fn addresses_one_row<S: DbScope, C: AddressesOneRow<WidgetRowDao, S>>(_condition: &C) {}

    #[test]
    fn test_key_from_base_addresses_one_row() {
        addresses_one_row::<Global, _>(&Name::try_from("w1").unwrap());
    }
}
