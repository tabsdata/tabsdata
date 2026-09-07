//
// Copyright 2024 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::collection::{ActiveCollectionDBWithNames, CollectionRead};
use td_objects::dxo::crudl::{ReadRequest, WithinProject};
use td_queries::LiveEntry;

#[service_factory]
async fn read_collection(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ReadRequest<CollectionParam>>,
) -> Result<CollectionRead, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, param) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);
    let id_name = param.collection;
    let with_names: ActiveCollectionDBWithNames = conn.live().fetch_one(&id_name).await?;
    CollectionRead::from_dao(with_names)
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_objects::dxo::collection::CollectionParam;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::CollectionIdName;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, CollectionName, Description, UserId, UserName,
    };
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_read_collection(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let before = AtTime::now();
        let name = CollectionName::try_from("ds0").unwrap();
        let _ = db
            .scoped(p.id)
            .seed()
            .collection(&name, &UserId::admin())
            .await;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .read(CollectionParam {
                collection: CollectionIdName::from_str(&name.to_string()).unwrap(),
            })
            .in_project(p.id);

        let service = Services::with_defaults(db);
        let response = service.read_collection(request).await;
        assert!(response.is_ok());
        let created = response.unwrap();

        assert_eq!(created.name, name);
        assert_eq!(created.description, Description::default());
        assert!(created.created_on >= before);
        assert_eq!(created.created_by_id, UserId::admin());
        assert_eq!(created.created_by, UserName::admin());
        assert_eq!(created.modified_on, created.created_on);
        assert_eq!(created.modified_by_id, UserId::admin());
        assert_eq!(created.modified_by, UserName::admin());
    }
}
