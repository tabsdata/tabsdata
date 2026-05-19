//
// Copyright 2024 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, NoPermissions, System};
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::collection::{CollectionDBWithNames, CollectionRead, CollectionReadBuilder};
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::rest_urls::CollectionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::CollectionIdName;
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ReadCollectionService,
    request = ReadRequest<CollectionParam>,
    response = CollectionRead,
    connection = ConnectionProvider,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ReadRequest<CollectionParam>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<NoPermissions>::check), // no permission required
        from_fn(With::<ReadRequest<CollectionParam>>::extract_name::<CollectionParam>),
        from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDBWithNames>),
        from_fn(With::<CollectionDBWithNames>::try_convert_to::<CollectionReadBuilder, _>),
        from_fn(With::<CollectionReadBuilder>::build::<CollectionRead>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::rest_urls::CollectionParam;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, CollectionName, Description, RoleId, UserId, UserName,
    };
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_read_collection(db: TdPool) {
        use td_tower::metadata::type_of_val;

        ReadCollectionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ReadRequest<CollectionParam>, CollectionRead>(&[
                type_of_val(&With::<ReadRequest<CollectionParam>>::extract::<RequestContext>),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<NoPermissions>::check), // no permission required
                type_of_val(&With::<ReadRequest<CollectionParam>>::extract_name::<CollectionParam>),
                type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
                type_of_val(&By::<CollectionIdName>::fetch_one::<CollectionDBWithNames>),
                type_of_val(
                    &With::<CollectionDBWithNames>::try_convert_to::<CollectionReadBuilder, _>,
                ),
                type_of_val(&With::<CollectionReadBuilder>::build::<CollectionRead>),
            ]);
    }

    #[td_test::test(db)]
    async fn test_read_collection(db: TdPool) {
        let before = AtTime::now();
        let name = CollectionName::try_from("ds0").unwrap();
        let _ = db.seed().collection(&name, &UserId::admin()).await;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).read(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(&name.to_string()).unwrap())
                    .build()
                    .unwrap(),
            );

        let service = ReadCollectionService::with_defaults(db).service();
        let response = service.raw_oneshot(request).await;
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
