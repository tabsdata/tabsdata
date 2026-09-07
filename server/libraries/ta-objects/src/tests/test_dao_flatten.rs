//
// Copyright 2026 Tabsdata Inc.
//

/// Tests for `#[dao(flatten)]`: an embedded base DAO whose columns the
/// enclosing DAO also projects, base first, via delegation.
#[cfg(test)]
mod tests {
    use crate as ta_objects;
    use tm_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};

    use crate::dxo::{Dao, DaoCore, DaoUpdate, KeyedDao, Recursive, TypedColumns, VersionedDao};
    use sea_query::{Iden, Order};
    use std::any::TypeId;
    use std::str::FromStr;

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct WidgetId(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct Name(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct DefinedOn(i64);

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
    #[dao(sql_table = "widgets", key(name))]
    struct WidgetDao {
        pub id: WidgetId,
        pub name: Name,
        pub defined_on: Option<DefinedOn>,
    }

    #[ta_objects::Dao]
    #[derive(Eq, PartialEq)]
    #[dao(sql_table = "widgets__with_names")]
    struct WidgetDaoWithNames {
        #[dao(flatten)]
        pub base: WidgetDao,
        pub owner: OwnerName,
        pub renamed: RenamedName,
    }

    #[ta_objects::Dao]
    #[derive(Eq, PartialEq)]
    #[dao(sql_table = "widgets__with_status")]
    struct WidgetDaoWithStatus {
        #[dao(flatten)]
        pub base: WidgetDaoWithNames,
        pub started_on: DefinedOn,
    }

    fn col_names<D: Dao>() -> Vec<String> {
        D::columns()
            .into_iter()
            .map(|c| Iden::to_string(&c))
            .collect()
    }

    #[test]
    fn test_table_and_column_rendering() {
        assert_eq!(
            Iden::to_string(&WidgetDaoWithNames::table()),
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

        // Two levels down.
        let deep = WidgetDaoWithStatusIden::from_str("id").unwrap();
        assert_eq!(Iden::to_string(&deep), "id");
    }

    #[test]
    fn test_col_for_type_resolves_base_and_own() {
        let col = WidgetDaoWithNames::col_for_type(TypeId::of::<Name>()).unwrap();
        assert_eq!(col, WidgetDaoWithNamesIden::Base(WidgetDaoIden::Name));

        let own = WidgetDaoWithNames::col_for_type(TypeId::of::<OwnerName>()).unwrap();
        assert_eq!(own, WidgetDaoWithNamesIden::Owner);
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
        // Two flatten levels down.
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
                defined_on: Some(DefinedOn::try_from(1i64).unwrap()),
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

    #[test]
    fn test_update_values_delegate_base() {
        let dao = WidgetDaoWithNames {
            base: WidgetDao {
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
        // `defined_on` is None (skipped), base columns come first.
        assert_eq!(cols, ["id", "name", "owner", "renamed"]);
    }

    // ---- versioned / recursive / keyed columns resolving into the base ----

    #[ta_objects::Dao]
    #[derive(Eq, PartialEq)]
    #[dao(
        sql_table = "versioned_widgets",
        versioned(pk = "id", version_by = "defined_on", partition_by = "widget_id"),
        recursive(up = "widget_id", down = "id")
    )]
    struct VersionedWidgetDao {
        pub id: WidgetVersionId,
        pub widget_id: WidgetId,
        pub defined_on: DefinedOn,
    }

    #[ta_objects::Dao]
    #[derive(Eq, PartialEq)]
    #[dao(
        sql_table = "versioned_widgets__with_names",
        versioned(
            pk = "id",
            version_by = "defined_on",
            partition_by = "widget_id",
            current = "versioned_widgets_current__with_names"
        ),
        recursive(up = "widget_id", down = "id")
    )]
    struct VersionedWidgetDaoWithNames {
        #[dao(flatten)]
        pub base: VersionedWidgetDao,
        pub owner: OwnerName,
    }

    #[test]
    fn test_versioned_cols_resolve_into_base() {
        assert_eq!(
            Iden::to_string(&VersionedWidgetDaoWithNames::pk_col()),
            "id"
        );
        assert_eq!(
            Iden::to_string(&VersionedWidgetDaoWithNames::partition_col()),
            "widget_id"
        );
        assert_eq!(
            Iden::to_string(&VersionedWidgetDaoWithNames::version_order_col()),
            "defined_on"
        );
    }

    #[test]
    fn test_recursive_cols_resolve_into_base() {
        assert_eq!(
            Iden::to_string(&VersionedWidgetDaoWithNames::recurse_up()),
            "widget_id"
        );
        assert_eq!(
            Iden::to_string(&VersionedWidgetDaoWithNames::recurse_down()),
            "id"
        );
    }

    #[ta_objects::Dao]
    #[derive(Eq, PartialEq)]
    #[dao(sql_table = "widgets_rows", key(name))]
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
}
