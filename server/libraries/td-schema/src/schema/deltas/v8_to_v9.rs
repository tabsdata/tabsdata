//
// Copyright 2026 Tabsdata Inc.
//

//! v8 → v9: `groups` and `connections` are keyed by `id` alone and their
//! referrers drop the composite FK; `groups.kind` replaces the well-known ids
//! as the handle on a project's system groups.
//!
//! The seeded system groups carried the same fixed id in every project. The
//! oldest project keeps them; every other project's are re-minted as the
//! project's own id with the last character swapped for a per-group marker.
//! Only the top three bits of that character are data (the low two are
//! base32 padding), so the result stays a valid UUIDv7 `Id` — derived from
//! one the application minted, so nothing here has to encode one. The
//! primary key added afterwards is the collision guard.

use sea_query::extension::postgres::PgExpr;
use sea_query::{Alias, ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Func, Iden, IntoIden, Query, SubQueryStatement, Table};

use crate::schema::render::{render_query, render_schema, update_db_version};
use crate::schema::version::SchemaDelta;

#[derive(Iden, Clone, Copy)]
enum Groups {
    Table,
    Id,
    ProjectId,
    Kind,
}

#[derive(Iden, Clone, Copy)]
enum Connections {
    Table,
    Id,
}

#[derive(Iden, Clone, Copy)]
enum Collections {
    Table,
    ConnectionId,
}

#[derive(Iden, Clone, Copy)]
enum Referrers {
    GroupId,
    ProjectId,
}

#[derive(Iden)]
struct Left;

const SYSTEM_GROUPS: &[(&str, &str, &str)] = &[
    ("00000000000000000000000208", "4", "default"),
    ("0000000000000000000000020c", "8", "sources"),
    ("0000000000000000000000020g", "c", "destinations"),
];

const GROUP_REFERRERS: &[(&str, &str)] = &[
    ("collections", "collections_project_id_group_id_fkey"),
    ("logical_function_runs", "logical_function_runs_project_id_group_id_fkey"),
    ("logical_table_data_versions", "logical_table_data_versions_project_id_group_id_fkey"),
    ("transactions", "transactions_project_id_group_id_fkey"),
];

const CONNECTION_FK: &str = "collections_project_id_connection_id_fkey";

fn system_ids() -> impl Iterator<Item = &'static str> {
    SYSTEM_GROUPS.iter().map(|(id, _, _)| *id)
}

fn keeper_project() -> Expr {
    Expr::from(SubQueryStatement::from(
        Query::select()
            .expr(Func::min(Expr::col(Groups::ProjectId)))
            .from(Groups::Table)
            .and_where(Expr::col(Groups::Id).is_in(system_ids()))
            .take(),
    ))
}

fn re_minted_rows(key: impl IntoIden + Copy, project: impl IntoIden + Copy) -> Expr {
    Expr::col(key).is_in(system_ids()).and(Expr::col(project).ne(keeper_project()))
}

fn re_minted_id(key: impl IntoIden + Copy, project: impl IntoIden + Copy) -> Expr {
    let mut marker = Expr::case(Expr::col(key).eq(SYSTEM_GROUPS[0].0), SYSTEM_GROUPS[0].1);
    for (id, m, _) in &SYSTEM_GROUPS[1..] {
        marker = marker.case(Expr::col(key).eq(*id), *m);
    }
    Expr::expr(Func::cust(Left).arg(Expr::col(project)).arg(25)).concatenate(marker)
}

fn kind_of_id() -> Expr {
    let mut kind = Expr::case(Expr::col(Groups::Id).eq(SYSTEM_GROUPS[0].0), SYSTEM_GROUPS[0].2);
    for (id, _, k) in &SYSTEM_GROUPS[1..] {
        kind = kind.case(Expr::col(Groups::Id).eq(*id), *k);
    }
    kind.finally("custom").into()
}

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 8;
    const TO_VERSION: usize = 9;

    fn retired() -> Vec<String> {
        [
            "collections___project_id__group_id___idx",
            "groups___id___idx",
            "logical_function_runs___project_id__group_id___idx",
            "logical_table_data_versions___project_id__group_id___idx",
            "transactions___project_id__group_id___idx",
        ]
        .iter()
        .map(|name| format!(r#"DROP INDEX IF EXISTS "{name}""#))
        .collect()
    }

    fn up() -> Vec<String> {
        let mut out: Vec<String> = GROUP_REFERRERS
            .iter()
            .map(|(table, fk)| render_schema(Table::alter().table(Alias::new(*table)).drop_constraint(Alias::new(*fk)).to_owned()))
            .collect();
        out.push(render_schema(Table::alter().table(Collections::Table).drop_constraint(Alias::new(CONNECTION_FK)).to_owned()));

        out.push(render_schema(Table::alter().table(Groups::Table).add_column(ColumnDef::new(Groups::Kind).text().null()).to_owned()));
        out.push(render_query(Query::update().table(Groups::Table).value(Groups::Kind, kind_of_id()).to_owned()));
        out.push(render_schema(
            Table::alter().table(Groups::Table).modify_column(ColumnDef::new(Groups::Kind).text().not_null()).to_owned(),
        ));

        for (table, _) in GROUP_REFERRERS {
            out.push(render_query(
                Query::update()
                    .table(Alias::new(*table))
                    .value(Referrers::GroupId, re_minted_id(Referrers::GroupId, Referrers::ProjectId))
                    .and_where(re_minted_rows(Referrers::GroupId, Referrers::ProjectId))
                    .to_owned(),
            ));
        }
        out.push(render_query(
            Query::update()
                .table(Groups::Table)
                .value(Groups::Id, re_minted_id(Groups::Id, Groups::ProjectId))
                .and_where(re_minted_rows(Groups::Id, Groups::ProjectId))
                .to_owned(),
        ));

        out.push(render_schema(Table::alter().table(Groups::Table).drop_constraint(Alias::new("groups_pkey")).to_owned()));
        out.push(r#"ALTER TABLE "groups" ADD PRIMARY KEY ("id")"#.to_string());
        out.push(render_schema(Table::alter().table(Connections::Table).drop_constraint(Alias::new("connections_pkey")).to_owned()));
        out.push(r#"ALTER TABLE "connections" ADD PRIMARY KEY ("id")"#.to_string());
        out.extend(GROUP_REFERRERS.iter().map(|(table, _)| {
            render_schema(
                Table::alter()
                    .table(Alias::new(*table))
                    .add_foreign_key(
                        ForeignKey::create()
                            .from(Alias::new(*table), Referrers::GroupId)
                            .to(Groups::Table, Groups::Id)
                            .on_delete(ForeignKeyAction::Restrict)
                            .on_update(ForeignKeyAction::Restrict)
                            .get_foreign_key(),
                    )
                    .to_owned(),
            )
        }));
        out.push(render_schema(
            Table::alter()
                .table(Collections::Table)
                .add_foreign_key(
                    ForeignKey::create()
                        .from(Collections::Table, Collections::ConnectionId)
                        .to(Connections::Table, Connections::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict)
                        .get_foreign_key(),
                )
                .to_owned(),
        ));

        out.push(update_db_version(Self::TO_VERSION));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::{Migration, SYSTEM_GROUPS};
    use crate::schema::tests::validate_delta;
    use td_common::id::{Id, id_time};

    #[test]
    fn test_validate_delta_v8_to_v9() {
        validate_delta::<Migration>();
    }

    /// The SQL re-mint is `left(project_id, 25) || marker`; the same surgery
    /// on a freshly minted project id must yield three distinct ids that
    /// parse and still read as UUIDv7 stamped at the project's instant.
    #[test]
    fn test_re_minted_ids_are_uuidv7_of_the_project() {
        let project = Id::default();
        let stem = &project.to_string()[..25];
        let mut minted: Vec<Id> = SYSTEM_GROUPS
            .iter()
            .map(|(_, marker, _)| Id::try_from(format!("{stem}{marker}").as_str()).expect("re-minted id must parse"))
            .collect();
        for id in &minted {
            assert_eq!(id_time(id), id_time(&project), "{id} must keep the project's timestamp");
        }
        minted.sort();
        minted.dedup();
        assert_eq!(minted.len(), SYSTEM_GROUPS.len(), "the markers must yield distinct ids");
    }

    #[test]
    fn test_fixed_ids_are_valid_ids() {
        for (id, _, _) in SYSTEM_GROUPS {
            Id::try_from(*id).expect("a v7 fixed id must parse");
        }
    }
}
