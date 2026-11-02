//
// Copyright 2026 Tabsdata Inc.
//

use crate::ui_data::UiDataError;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::ui_data::UserUiDataDB;
use td_queries::Queries;

#[service_factory]
async fn get_ui_data(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO>,
    ctx: RequestContext,
) -> Result<UserUiDataDB, TdError> {
    let conn = gated_conn.get(authz.none());
    match conn.fetch_optional::<UserUiDataDB>(&ctx.user_id).await? {
        Some(row) => Ok(row),
        None => Err(UiDataError::UiDataNotFound)?,
    }
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use crate::ui_data::UiDataError;
    use td_database::pool::TdPool;
    use td_error::assert_service_error;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::ui_data::UserUiDataDB;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, UiDataDocument, UiDataEtag, UserEnabled, UserName,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_get_ui_data(db: TdPool) {
        let user = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::TRUE)
            .await
            .unwrap();
        let row = UserUiDataDB {
            user_id: user.id,
            etag: UiDataEtag::mint(),
            modified_on: AtTime::default().into(),
            data: UiDataDocument::try_from(r#"{"b": 1,  "a":[2]}"#).unwrap(),
        };
        db.insert(&row).await.unwrap();

        let service = Services::with_defaults(db.clone());
        let context = RequestContext::with(AccessTokenId::default(), user.id);
        let found = service.get_ui_data(context).await.unwrap().data;
        assert_eq!(found.etag, row.etag);
        assert_eq!(found.data, row.data);
    }

    #[td_test::test(db)]
    async fn test_get_ui_data_not_found(db: TdPool) {
        let user = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::TRUE)
            .await
            .unwrap();

        let service = Services::with_defaults(db.clone());
        let context = RequestContext::with(AccessTokenId::default(), user.id);
        assert_service_error(service.get_ui_data(context).await, |err| match err {
            UiDataError::UiDataNotFound => {}
            other => panic!("Expected 'UiDataNotFound', got {other:?}"),
        });
    }
}
