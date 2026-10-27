//
// Copyright 2026 Tabsdata Inc.
//

//! v19 → v20 gives every database a random `instance_id`, kept for its
//! lifetime.

use crate::components::ApiserverComponent;
use crate::pool::TdPool;
use crate::test_utils::domain::PostgresDomain;
use crate::test_utils::postgres::pool;
use td_schema::schema::archive;

use super::scalars;

async fn uuid_instance_id(db: &TdPool) -> String {
    let id = db.instance_id().await.expect("reading instance_id");
    let parsed = scalars(db, format!("SELECT '{id}'::uuid::text")).await;
    assert_eq!(
        parsed,
        std::slice::from_ref(&id),
        "instance_id is not a UUID"
    );
    id
}

#[td_test::domain("apiserver_postgres_internals")]
#[tokio::test]
async fn fresh_install_keeps_its_instance_id_postgres(
    domain_apiserver_postgres_internals: PostgresDomain,
) {
    let db = pool(&domain_apiserver_postgres_internals).await;
    db.upgrade().await.unwrap();
    let id = uuid_instance_id(&db.serving).await;

    db.upgrade().await.unwrap();
    assert_eq!(db.serving.instance_id().await.unwrap(), id);
}

#[td_test::domain("apiserver_postgres_internals")]
#[tokio::test]
async fn v19_to_v20_mints_an_instance_id_postgres(
    domain_apiserver_postgres_internals: PostgresDomain,
) {
    let db = pool(&domain_apiserver_postgres_internals).await;
    db.admin
        .upgrade_source(archive::source(19))
        .await
        .expect("installing archive v19");
    assert!(db.serving.instance_id().await.is_err());

    db.admin
        .upgrade(&ApiserverComponent)
        .await
        .expect("upgrading v19 -> current");
    uuid_instance_id(&db.serving).await;
}

#[td_test::domain("apiserver_postgres_internals")]
#[tokio::test]
async fn fresh_installs_get_distinct_instance_ids_postgres(
    domain_apiserver_postgres_internals: PostgresDomain,
) {
    let first = pool(&domain_apiserver_postgres_internals).await;
    first.upgrade().await.unwrap();
    let second = pool(&domain_apiserver_postgres_internals).await;
    second.upgrade().await.unwrap();
    assert_ne!(
        first.serving.instance_id().await.unwrap(),
        second.serving.instance_id().await.unwrap()
    );
}
