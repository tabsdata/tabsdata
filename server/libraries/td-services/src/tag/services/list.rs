//
// Copyright 2026 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::dxo::crudl::{ListRequest, ListResponse, WithinProject};
use td_objects::dxo::tag::Tag;
use td_queries::Queries;

#[service_factory]
async fn tag_list(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ListRequest>,
) -> Result<ListResponse<Tag>, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, _, list_params) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);
    conn.scd2().list((), list_params).await
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionName, ProjectName, ReuseFrozen,
        TagEntityId, TagEntityType, TagKey, TagKind, TagValue, TopologyTagKey, UserId,
    };
    use td_queries::test_utils::SeedEntry;

    fn publisher(name: &str) -> Result<FunctionRegister, TdError> {
        Ok(FunctionRegister {
            child_functions: None,
            name: FunctionName::try_from(name)?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            parent_tables: None,
            runtime_values: "rv".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        })
    }

    #[td_test::test(db)]
    async fn test_list_tags_finds_children_by_parent(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.project_id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;
        let owner = db
            .scoped(p.project_id)
            .seed()
            .function(&collection, &publisher("owner")?)
            .await?;
        let child = db
            .scoped(p.project_id)
            .seed()
            .child_function(&collection, &publisher("child")?, &owner.function_id)
            .await?;
        let other = db.seed().project(ProjectName::try_from("other")?).await?;

        let service = Services::with_defaults(db.clone());
        let list = |filter: Vec<String>| {
            service.tag_list(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .list(
                        (),
                        ListParams {
                            filter,
                            ..ListParams::default()
                        },
                    )
                    .in_project(p.project_id),
            )
        };
        let parent = TagValue::of_id(owner.function_id);

        let all = list(vec![]).await?.data.data;
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].entity_type, TagEntityType::Function);
        assert_eq!(all[0].entity_id, TagEntityId::try_from(child.function_id)?);
        assert_eq!(all[0].kind, TagKind::Topology);
        assert_eq!(all[0].key, TagKey::from(TopologyTagKey::Parent));
        assert_eq!(all[0].value, parent);

        let children = list(vec![
            "entity_type:eq:F".to_string(),
            format!("key:eq:{}", TagKey::from(TopologyTagKey::Parent)),
            format!("value:eq:{}", *parent),
        ])
        .await?
        .data
        .data;
        assert_eq!(children.len(), 1);
        assert_eq!(
            children[0].entity_id,
            TagEntityId::try_from(child.function_id)?
        );

        let none = list(vec!["entity_type:eq:T".to_string()]).await?.data.data;
        assert!(none.is_empty());

        let elsewhere = service
            .tag_list(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .list((), ListParams::default())
                    .in_project(other.project_id),
            )
            .await?
            .data
            .data;
        assert!(elsewhere.is_empty());
        Ok(())
    }
}
