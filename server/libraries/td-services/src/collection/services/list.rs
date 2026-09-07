//
// Copyright 2024 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_objects::dxo::collection::CollectionRead;
use td_objects::dxo::crudl::{ListRequest, ListResponse};
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_queries::LiveEntry;

#[service_factory]
async fn list_collections(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO>,
    request: ListRequest,
) -> Result<ListResponse<CollectionRead>, TdError> {
    let (ctx, _, list_params) = request.into_parts();
    let (grant, visible) = authz.visible_projects(gated_conn, &ctx).await?;
    let query_params = ListQueryParams::try_from(&list_params)?;
    let rows = gated_conn
        .get(grant)
        .live()
        .list(&visible, &query_params)
        .await?;
    build_list_response(&list_params, &query_params, rows)
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::types::basic::{
        AccessTokenId, CollectionName, Description, ProjectName, RoleName, UserEnabled, UserId,
        UserName,
    };
    use td_queries::test_utils::{SeedEntry, seed_project_viewer};

    #[td_test::test(db)]
    async fn test_list_collection(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let name = CollectionName::try_from("ds0").unwrap();
        let _ = db
            .scoped(p.id)
            .seed()
            .collection(&name, &UserId::admin())
            .await
            .unwrap();

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list((), ListParams::default());

        let service = Services::with_defaults(db);
        let response = service.list_collections(request).await;
        assert!(response.is_ok());
        let list = response.unwrap().data;
        assert_eq!(list.len, 1);
        assert_eq!(list.data[0].name, name);
    }

    #[td_test::test(db)]
    async fn test_list_collection_unauthorized(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        // Create new role without permissions
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

        let name = CollectionName::try_from("ds0")?;
        let _ = db
            .scoped(p.id)
            .seed()
            .collection(&name, &UserId::admin())
            .await?;

        // Every collection of a viewable project is listed (the admin user
        // holds sys_admin, which grants manage_project on all projects).
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list((), ListParams::default());

        let service = Services::with_defaults(db.clone());
        let response = service.list_collections(request).await;
        assert!(response.is_ok());
        let list = response?.data;
        assert_eq!(list.len, 1);
        assert_eq!(list.data[0].name, name);

        // A user whose roles grant view_project on no project sees nothing.
        let request =
            RequestContext::with(AccessTokenId::default(), user.id).list((), ListParams::default());

        let service = Services::with_defaults(db.clone());
        let response = service.list_collections(request).await?.data;
        assert_eq!(response.len, 0);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_collections_spans_projects(db: TdPool) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        db.scoped(base.id)
            .seed()
            .collection(&CollectionName::try_from("c_base")?, &UserId::admin())
            .await?;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        db.scoped(alpha.id)
            .seed()
            .collection(&CollectionName::try_from("c_alpha")?, &UserId::admin())
            .await?;

        let service = Services::with_defaults(db.clone());
        let response = service
            .list_collections(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .list((), ListParams::default()),
            )
            .await?
            .data;

        let projects: Vec<_> = response.data.iter().map(|c| c.project.clone()).collect();
        assert!(projects.contains(&base.name));
        assert!(projects.contains(&ProjectName::try_from("alpha")?));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_collections_filters_to_viewable(db: TdPool) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        db.scoped(base.id)
            .seed()
            .collection(&CollectionName::try_from("c_base")?, &UserId::admin())
            .await?;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        db.scoped(alpha.id)
            .seed()
            .collection(&CollectionName::try_from("c_alpha")?, &UserId::admin())
            .await?;

        let user = seed_project_viewer(&db, base.id).await?;

        let service = Services::with_defaults(db.clone());
        let response = service
            .list_collections(
                RequestContext::with(AccessTokenId::default(), user.id)
                    .list((), ListParams::default()),
            )
            .await?
            .data;

        let projects: Vec<_> = response.data.iter().map(|c| c.project.clone()).collect();
        assert!(projects.contains(&base.name));
        assert!(!projects.contains(&ProjectName::try_from("alpha")?));
        Ok(())
    }
}
