//
// Copyright 2026 Tabsdata Inc.
//

//! A bundle upload holds no database transaction while the client is still
//! sending the bundle.

use crate::Services;
use axum::body::Body;
use axum::extract::Request;
use futures::stream;
use std::str::FromStr;
use td_database::access::{AcrossProjects, DbPool, Grant};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::TdError;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::function_upload::FunctionUpload;
use td_objects::types::basic::{AccessTokenId, CollectionIdName, CollectionName, UserId};
use td_queries::test_utils::SeedEntry;
use td_queries::test_utils::probes::locks_held_by_others;
use tokio::sync::oneshot;

/// The client has sent the first part of a bundle and is slow with
/// the rest. While the body is still streaming, the upload must hold no
/// transaction (seen as no other backend holding a lock on this test's
/// `collections`, which the upload reads); the database work belongs before
/// or after the transfer, not around it.
#[td_test::test(db)]
async fn test_upload_holds_no_transaction_while_the_body_streams(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    db.scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_up")?, &UserId::admin())
        .await?;

    let (reached, reached_rx) = oneshot::channel::<()>();
    let (release, release_rx) = oneshot::channel::<()>();
    let body = stream::unfold(
        (0, Some(reached), Some(release_rx)),
        |(part, mut reached, mut release_rx)| async move {
            match part {
                0 => Some((
                    Ok::<_, std::io::Error>(b"first part".to_vec()),
                    (1, reached, release_rx),
                )),
                1 => {
                    let _ = reached.take().expect("reached once").send(());
                    let _ = release_rx.take().expect("released once").await;
                    Some((Ok(b"last part".to_vec()), (2, None, None)))
                }
                _ => None,
            }
        },
    );
    let upload = FunctionUpload::new(
        Request::builder()
            .body(Body::from_stream(body))
            .expect("request"),
    );
    let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .create(
            CollectionParam {
                collection: CollectionIdName::from_str("c_up")?,
            },
            upload,
        )
        .in_project(p.id);
    let services = Services::with_defaults(db.clone());
    let uploading = tokio::spawn(async move { services.upload_function(request).await });
    reached_rx
        .await
        .expect("the upload never read past the first part");

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut probe = pools.read().await?;
    let held = locks_held_by_others(probe.get(Grant::asserted("lock probe")), "collections")
        .await
        .expect("lock probe");
    drop(probe);

    let _ = release.send(());
    uploading.await.expect("upload task")?;
    assert_eq!(
        held, 0,
        "the upload held a transaction open while the client was still sending"
    );
    Ok(())
}
