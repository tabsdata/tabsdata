//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::dxo::crudl::{ListRequest, ListResponse, WithinProject};
use td_objects::dxo::function::Function;
use td_queries::Queries;

#[service_factory]
async fn function_list(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ListRequest>,
) -> Result<ListResponse<Function>, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, _, list_params) = request.into_parts();
    // Within the project every collection is visible (no cross-project visibility).
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);
    conn.scd2().list((), list_params).await
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use td_authz::rbac::RbacError;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::dxo::crudl::ListParams;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, Description, FunctionName, RoleName,
        UserEnabled, UserId, UserName,
    };
    use td_objects::types::basic::{ReuseFrozen, TagValue};
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_list_current_functions(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.project_id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "function_1".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
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

        let create = FunctionRegister {
            child_functions: None,
            name: "function_2".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
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
            .list((), ListParams::default())
            .in_project(p.project_id);

        let service = Services::with_defaults(db.clone());
        let response = service.function_list(request).await;
        let response = response?.data;
        let data = response.data;

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name, FunctionName::try_from("function_1")?);
        assert_eq!(data[1].name, FunctionName::try_from("function_2")?);
        Ok(())
    }

    fn publisher(name: &str) -> Result<FunctionRegister, TdError> {
        Ok(FunctionRegister {
            child_functions: None,
            name: name.try_into()?,
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
            runtime_values: "mock runtime values".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        })
    }

    #[td_test::test(db)]
    async fn test_list_functions_by_parent_tag(db: TdPool) -> Result<(), TdError> {
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
        db.scoped(p.project_id)
            .seed()
            .child_function(&collection, &publisher("child")?, &owner.function_id)
            .await?;

        let service = Services::with_defaults(db.clone());
        let project = p.project_id;
        let names = |filter: Vec<String>| {
            let service = &service;
            async move {
                let listed = service
                    .function_list(
                        RequestContext::with(AccessTokenId::default(), UserId::admin())
                            .list(
                                (),
                                ListParams {
                                    filter,
                                    ..ListParams::default()
                                },
                            )
                            .in_project(project),
                    )
                    .await?
                    .data
                    .data;
                Ok::<_, TdError>(
                    listed
                        .into_iter()
                        .map(|f| f.name.to_string())
                        .collect::<Vec<_>>(),
                )
            }
        };
        let parent = TagValue::of_id(owner.function_id);

        assert_eq!(
            names(vec![format!("function._parent:eq:{}", *parent)]).await?,
            ["child"]
        );
        assert_eq!(
            names(vec!["function._parent:is:not_null".to_string()]).await?,
            ["child"]
        );
        assert_eq!(
            names(vec!["function._parent:is:null".to_string()]).await?,
            ["owner"]
        );
        assert!(
            names(vec![
                format!("function._parent:eq:{}", *parent),
                "name:eq:owner".to_string()
            ])
            .await?
            .is_empty()
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_function_versions_without_permissions(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let user = db
            .seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::TRUE)
            .await?;
        let role = db
            .seed()
            .role(
                RoleName::try_from("unauthorized_role")?,
                Description::try_from("any user")?,
            )
            .await?;
        let _user_role = db.seed().user_role(&user.id, &role.id).await?;

        let collection = db
            .scoped(p.project_id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "function_1".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
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

        let create = FunctionRegister {
            child_functions: None,
            name: "function_2".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
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
            .list((), ListParams::default())
            .in_project(p.project_id);

        let service = Services::with_defaults(db.clone());
        let response = service.function_list(request).await;
        let response = response?.data;
        let data = response.data;

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name, FunctionName::try_from("function_1")?);
        assert_eq!(data[1].name, FunctionName::try_from("function_2")?);

        let request = RequestContext::with(AccessTokenId::default(), user.id)
            .list((), ListParams::default())
            .in_project(p.project_id);

        let service = Services::with_defaults(db.clone());
        let response = service.function_list(request).await;
        let err = response.expect_err("unauthorized user must be forbidden");
        assert!(matches!(err.domain_err(), RbacError::Forbidden(_, _)));
        Ok(())
    }
}
