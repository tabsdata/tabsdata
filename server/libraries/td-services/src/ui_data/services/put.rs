//
// Copyright 2026 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_database::access::{DbPool, Global, retrying};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::crudl::UpdateRequest;
use td_objects::dxo::ui_data::{
    UiDataPrecondition, UiDataPut, UiDataWrite, UserUiDataDB, UserUiDataUpdateDB,
};
use td_objects::types::basic::UiDataEtag;
use td_queries::Queries;

#[service_factory]
async fn put_ui_data(
    authz: Ctx<AuthzContext>,
    db: DbPool<Global>,
    request: UpdateRequest<UiDataPut>,
) -> Result<UiDataWrite, TdError> {
    let (ctx, (), put) = request.into_parts();
    retrying(|attempt| async {
        let mut trx = db.begin(attempt).await?;
        let conn = trx.get(authz.none());
        let row = UserUiDataDB {
            user_id: ctx.user_id,
            etag: UiDataEtag::mint(),
            modified_on: ctx.time.into(),
            data: put.data.clone(),
        };
        let replacement = UserUiDataUpdateDB::from(&row);
        let precondition = if put.force {
            None
        } else {
            Some(&put.precondition)
        };

        let write = match precondition {
            None => {
                if conn.insert_or_ignore(&row).await?.rows_affected() == 1 {
                    UiDataWrite::Created(row)
                } else {
                    conn.update(&replacement, &ctx.user_id).await?;
                    UiDataWrite::Replaced(row)
                }
            }
            Some(UiDataPrecondition::Absent) => {
                if conn.insert_or_ignore(&row).await?.rows_affected() == 1 {
                    UiDataWrite::Created(row)
                } else {
                    let current = conn.fetch_one::<UserUiDataDB>(&ctx.user_id).await?;
                    UiDataWrite::PreconditionRequired(current.etag)
                }
            }
            Some(UiDataPrecondition::Etag(etag)) => {
                let updated = conn
                    .update(&replacement, and!(&ctx.user_id, etag))
                    .await?
                    .rows_affected();
                if updated == 1 {
                    UiDataWrite::Replaced(row)
                } else {
                    let current = conn.fetch_optional::<UserUiDataDB>(&ctx.user_id).await?;
                    UiDataWrite::PreconditionFailed(current)
                }
            }
            Some(UiDataPrecondition::NeverMatches) => {
                let current = conn.fetch_optional::<UserUiDataDB>(&ctx.user_id).await?;
                UiDataWrite::PreconditionFailed(current)
            }
        };
        trx.commit().await?;
        Ok(write)
    })
    .await
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use td_database::pool::TdPool;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::ui_data::{UiDataPrecondition, UiDataPut, UiDataWrite, UserUiDataDB};
    use td_objects::types::basic::{
        AccessTokenId, UiDataDocument, UiDataEtag, UserEnabled, UserId, UserName,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    async fn user(db: &TdPool) -> UserId {
        db.seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::TRUE)
            .await
            .unwrap()
            .id
    }

    async fn put(
        service: &Services,
        user: UserId,
        data: &str,
        precondition: UiDataPrecondition,
        force: bool,
    ) -> UiDataWrite {
        let request = RequestContext::with(AccessTokenId::default(), user).update(
            (),
            UiDataPut {
                data: UiDataDocument::try_from(data).unwrap(),
                precondition,
                force,
            },
        );
        service.put_ui_data(request).await.unwrap().data
    }

    async fn stored(db: &TdPool, user: UserId) -> Option<UserUiDataDB> {
        db.fetch_optional::<UserUiDataDB>(&user).await.unwrap()
    }

    #[td_test::test(db)]
    async fn test_create_without_if_match(db: TdPool) {
        let user = user(&db).await;
        let service = Services::with_defaults(db.clone());

        let UiDataWrite::Created(created) = put(
            &service,
            user,
            r#"{"b": 1,  "a":[2]}"#,
            UiDataPrecondition::Absent,
            false,
        )
        .await
        else {
            panic!("expected Created");
        };
        let row = stored(&db, user).await.unwrap();
        assert_eq!(row.etag, created.etag);
        assert_eq!(&*row.data, r#"{"b": 1,  "a":[2]}"#);
    }

    #[td_test::test(db)]
    async fn test_replace_without_if_match_requires_precondition(db: TdPool) {
        let user = user(&db).await;
        let service = Services::with_defaults(db.clone());
        put(&service, user, "{}", UiDataPrecondition::Absent, false).await;
        let current = stored(&db, user).await.unwrap();

        let UiDataWrite::PreconditionRequired(etag) = put(
            &service,
            user,
            r#"{"x":1}"#,
            UiDataPrecondition::Absent,
            false,
        )
        .await
        else {
            panic!("expected PreconditionRequired");
        };
        assert_eq!(etag, current.etag);
        assert_eq!(&*stored(&db, user).await.unwrap().data, "{}");
    }

    #[td_test::test(db)]
    async fn test_replace_with_current_etag(db: TdPool) {
        let user = user(&db).await;
        let service = Services::with_defaults(db.clone());
        put(&service, user, "{}", UiDataPrecondition::Absent, false).await;
        let before = stored(&db, user).await.unwrap();

        let UiDataWrite::Replaced(replaced) = put(
            &service,
            user,
            r#"{"x":1}"#,
            UiDataPrecondition::Etag(before.etag),
            false,
        )
        .await
        else {
            panic!("expected Replaced");
        };
        let after = stored(&db, user).await.unwrap();
        assert_ne!(after.etag, before.etag);
        assert_eq!(after.etag, replaced.etag);
        assert_eq!(&*after.data, r#"{"x":1}"#);
    }

    #[td_test::test(db)]
    async fn test_replace_with_same_payload_issues_new_etag(db: TdPool) {
        let user = user(&db).await;
        let service = Services::with_defaults(db.clone());
        put(&service, user, "{}", UiDataPrecondition::Absent, false).await;
        let before = stored(&db, user).await.unwrap();

        put(
            &service,
            user,
            "{}",
            UiDataPrecondition::Etag(before.etag),
            false,
        )
        .await;
        assert_ne!(stored(&db, user).await.unwrap().etag, before.etag);
    }

    #[td_test::test(db)]
    async fn test_stale_etag_fails_with_current_document(db: TdPool) {
        let user = user(&db).await;
        let service = Services::with_defaults(db.clone());
        put(
            &service,
            user,
            r#"{"v":1}"#,
            UiDataPrecondition::Absent,
            false,
        )
        .await;
        let stale = stored(&db, user).await.unwrap().etag;
        put(
            &service,
            user,
            r#"{"v":2}"#,
            UiDataPrecondition::Etag(stale),
            false,
        )
        .await;

        let UiDataWrite::PreconditionFailed(Some(current)) = put(
            &service,
            user,
            r#"{"v":3}"#,
            UiDataPrecondition::Etag(stale),
            false,
        )
        .await
        else {
            panic!("expected PreconditionFailed with the current document");
        };
        assert_eq!(&*current.data, r#"{"v":2}"#);
        assert_eq!(current.etag, stored(&db, user).await.unwrap().etag);
    }

    #[td_test::test(db)]
    async fn test_if_match_without_document_fails(db: TdPool) {
        let user = user(&db).await;
        let service = Services::with_defaults(db.clone());

        let write = put(
            &service,
            user,
            "{}",
            UiDataPrecondition::Etag(UiDataEtag::mint()),
            false,
        )
        .await;
        assert!(matches!(write, UiDataWrite::PreconditionFailed(None)));
        assert!(stored(&db, user).await.is_none());
    }

    #[td_test::test(db)]
    async fn test_unmatchable_if_match_fails(db: TdPool) {
        let user = user(&db).await;
        let service = Services::with_defaults(db.clone());
        put(&service, user, "{}", UiDataPrecondition::Absent, false).await;

        let write = put(
            &service,
            user,
            r#"{"x":1}"#,
            UiDataPrecondition::NeverMatches,
            false,
        )
        .await;
        assert!(matches!(write, UiDataWrite::PreconditionFailed(Some(_))));
        assert_eq!(&*stored(&db, user).await.unwrap().data, "{}");
    }

    #[td_test::test(db)]
    async fn test_override_creates_and_replaces(db: TdPool) {
        let user = user(&db).await;
        let service = Services::with_defaults(db.clone());

        let write = put(&service, user, "{}", UiDataPrecondition::NeverMatches, true).await;
        assert!(matches!(write, UiDataWrite::Created(_)));

        let write = put(
            &service,
            user,
            r#"{"x":1}"#,
            UiDataPrecondition::Absent,
            true,
        )
        .await;
        assert!(matches!(write, UiDataWrite::Replaced(_)));
        assert_eq!(&*stored(&db, user).await.unwrap().data, r#"{"x":1}"#);
    }

    #[td_test::test(db)]
    async fn test_concurrent_writers_with_same_etag(db: TdPool) {
        let user = user(&db).await;
        let service = Services::with_defaults(db.clone());
        put(&service, user, "{}", UiDataPrecondition::Absent, false).await;
        let etag = stored(&db, user).await.unwrap().etag;

        let (a, b) = tokio::join!(
            put(
                &service,
                user,
                r#"{"w":"a"}"#,
                UiDataPrecondition::Etag(etag),
                false
            ),
            put(
                &service,
                user,
                r#"{"w":"b"}"#,
                UiDataPrecondition::Etag(etag),
                false
            ),
        );
        let replaced = [&a, &b]
            .iter()
            .filter(|w| matches!(w, UiDataWrite::Replaced(_)))
            .count();
        let failed = [&a, &b]
            .iter()
            .filter(|w| matches!(w, UiDataWrite::PreconditionFailed(Some(_))))
            .count();
        assert_eq!((replaced, failed), (1, 1));
    }
}
