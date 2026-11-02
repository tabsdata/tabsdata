//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{MaintainCatalog, Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::{AtTimeParam, ListRequest, ListResponse, WithinProject};
use td_objects::dxo::table::{Table, TableDBWithNames};
use td_queries::Queries;

#[service_factory]
async fn table_list_by_collection(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ListRequest<(CollectionParam, AtTimeParam)>>,
) -> Result<ListResponse<Table>, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (param, at), list_params) = request.into_parts();
    let grant = authz
        .require::<Project, (ViewProject, MaintainCatalog)>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);

    let collection_id_name = param.collection;
    let collection: CollectionDB = conn.scd2().fetch_one(&collection_id_name).await?;
    let condition = and!(&collection.collection_id, TableDBWithNames::user());
    match &at.at {
        None => conn.scd2().list(condition, list_params).await,
        Some(at) => conn.scd2().as_of(at).list(condition, list_params).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::Typed;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::TableParam;
    use td_objects::dxo::tag::TagSet;
    use td_objects::types::basic::ProjectId;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BundleId, CollectionName, Decorator, TableIdName, TableName,
        TableNameDto, TagValue, UserId, UserTagKey,
    };
    use td_objects::types::basic::{CollectionIdName, ReuseFrozen};
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_list_current_tables_by_collection(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.project_id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let tables = vec![
            TableNameDto::try_from("table_1")?,
            TableNameDto::try_from("table_2")?,
        ];
        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(tables),
            table_types: None,
            parent_tables: None,
            runtime_values: "mock runtime values".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let _ = db
            .scoped(p.project_id)
            .seed()
            .function(&collection, &create)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!(
                            "~{}",
                            collection.collection_id
                        ))?,
                    },
                    AtTimeParam { at: None },
                ),
                ListParams::default(),
            )
            .in_project(p.project_id);

        let service = Services::with_defaults(db.clone());
        let response = service.table_list_by_collection(request).await;
        let response = response?.data;
        let data = response.data;

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name, TableName::try_from("table_1")?);
        assert_eq!(data[1].name, TableName::try_from("table_2")?);
        Ok(())
    }

    fn collection_param(collection: &CollectionDB) -> Result<CollectionParam, TdError> {
        Ok(CollectionParam {
            collection: CollectionIdName::from_str(&format!("~{}", collection.collection_id))?,
        })
    }

    async fn set_stage(
        service: &Services,
        project: ProjectId,
        collection: &CollectionDB,
        value: &str,
    ) -> Result<(), TdError> {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                (
                    collection_param(collection)?,
                    TableParam {
                        table: TableIdName::from_str("table_1")?,
                    },
                ),
                TagSet {
                    key: UserTagKey::try_new("stage")?,
                    value: TagValue::try_new(value)?,
                },
            )
            .in_project(project);
        service.set_table_tag(request).await?;
        Ok(())
    }

    async fn names(
        service: &Services,
        project: ProjectId,
        collection: &CollectionDB,
        at: Option<AtTime>,
        filter: &[&str],
    ) -> Result<Vec<String>, TdError> {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                (collection_param(collection)?, AtTimeParam { at }),
                ListParams {
                    filter: filter.iter().map(|f| f.to_string()).collect(),
                    ..ListParams::default()
                },
            )
            .in_project(project);
        let tables = service.table_list_by_collection(request).await?.data.data;
        Ok(tables.into_iter().map(|t| t.name.to_string()).collect())
    }

    #[td_test::test(db)]
    async fn test_list_tables_by_collection_as_of_an_instant(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let project = p.project_id;
        let collection = db
            .scoped(project)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;
        let service = Services::with_defaults(db.clone());

        let before = AtTime::now();
        let register = FunctionRegister {
            child_functions: None,
            name: "joaquin".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![
                TableNameDto::try_from("table_1")?,
                TableNameDto::try_from("table_2")?,
            ]),
            table_types: None,
            parent_tables: None,
            runtime_values: "mock runtime values".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        db.scoped(project)
            .seed()
            .function(&collection, &register)
            .await?;
        set_stage(&service, project, &collection, "one").await?;
        let staged_one = AtTime::now();
        set_stage(&service, project, &collection, "two").await?;

        let at = |at, filter| names(&service, project, &collection, at, filter);
        let one = &["table.stage:eq:one"];
        let two = &["table.stage:eq:two"];
        assert!(at(Some(before), &[]).await?.is_empty());
        assert_eq!(at(Some(staged_one), &[]).await?, ["table_1", "table_2"]);
        assert_eq!(at(Some(staged_one), one).await?, ["table_1"]);
        assert!(at(Some(staged_one), two).await?.is_empty());
        assert!(at(None, one).await?.is_empty());
        assert_eq!(at(None, two).await?, ["table_1"]);
        Ok(())
    }
}
