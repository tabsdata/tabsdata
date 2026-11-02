//
// Copyright 2026 Tabsdata Inc.
//

use super::*;
use td_objects::types::basic::{Description, PermissionType, RoleName, UserEnabled, UserName};

/// A user holding `maintain_catalog`, as a catalog service identity does.
async fn seed_maintainer(db: &TdPool) -> UserId {
    let role = db
        .seed()
        .role(
            RoleName::try_from("test_maintainer").unwrap(),
            Description::try_from("maintain_catalog").unwrap(),
        )
        .await
        .unwrap();
    db.seed()
        .permission(PermissionType::maintain_catalog(), None, None, &role)
        .await
        .unwrap();
    let user = db
        .seed()
        .user(
            &UserName::try_from("test_maintainer").unwrap(),
            &UserEnabled::TRUE,
        )
        .await
        .unwrap();
    db.seed().user_role(&user.id, &role.id).await.unwrap();
    user.id
}

fn identity(user: UserId) -> RequestContext {
    RequestContext::service_token(AccessTokenId::default(), user, TokenScope::Identity)
}

fn de<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> T {
    serde_json::from_value(value).unwrap()
}

fn sample(limit: i32) -> ProbeSubmit {
    ProbeSubmit {
        kind: ProbeKind::Sample,
        entity: Some(EntityFqn::try_from("crm.public.invoices").unwrap()),
        entities: None,
        limit: Some(rows(limit)),
    }
}

fn sampling(db: &TdPool) -> Probing {
    configured(
        db,
        CatalogConfig {
            maintenance_samples: true,
            ..CatalogConfig::default()
        },
    )
}

/// `catalog.maintenance_samples` is off by default: a maintainer's sample is
/// refused however small, a developer's is not.
#[td_test::test(db)]
async fn maintenance_samples_are_refused_while_the_setting_is_off(db: TdPool) {
    let project = project_with_source(&db).await;
    let maintainer = seed_maintainer(&db).await;
    let services = services(&db, Orchestrator::Kubernetes);
    refresh(
        &services,
        project,
        HASH_A,
        vec![entity("crm.public.invoices", "id amount")],
    )
    .await;

    let error = submit(&services, identity(maintainer), project, sample(1))
        .await
        .unwrap_err();
    assert_eq!(error.api_error(), ApiError::Forbidden);
    assert!(
        error.to_string().contains("catalog.maintenance_samples"),
        "{error}"
    );
    submit(&services, admin(), project, sample(1))
        .await
        .unwrap();
}

/// `maintain_catalog` reaches every route the classification sweep calls,
/// in every project, with small samples only once
/// `catalog.maintenance_samples` is on; a delegated token cannot carry it,
/// and it opens no collection, execution or table data route.
#[td_test::test(db)]
async fn maintain_catalog_reaches_only_the_sweep_routes(db: TdPool) {
    let project = project_with_source(&db).await;
    let maintainer = seed_maintainer(&db).await;
    let services = sampling(&db);
    refresh(
        &services,
        project,
        HASH_A,
        vec![entity("crm.public.invoices", "id amount")],
    )
    .await;
    let me = || identity(maintainer);

    let listed = services
        .list_connections(me().list((), ListParams::default()))
        .await
        .unwrap()
        .data;
    assert_eq!(listed.len, 1);
    services
        .read_connection_catalog(me().read(collection_param("crm")).in_project(project))
        .await
        .unwrap();
    services
        .list_connection_entities(
            me().list(
                (
                    collection_param("crm"),
                    EntityListParams {
                        fields: IncludeFields::from(true),
                    },
                ),
                ListParams::default(),
            )
            .in_project(project),
        )
        .await
        .unwrap();
    services
        .read_collection(me().read(collection_param("crm")).in_project(project))
        .await
        .unwrap();
    services
        .table_list_by_collection(
            me().list(
                (collection_param("crm"), de(serde_json::json!({}))),
                ListParams::default(),
            )
            .in_project(project),
        )
        .await
        .unwrap();

    for refused in [
        sample(6),
        submit_of(ProbeKind::Metadata),
        submit_of(ProbeKind::Check),
    ] {
        assert_eq!(
            submit(&services, me(), project, refused)
                .await
                .unwrap_err()
                .api_error(),
            ApiError::Forbidden
        );
    }
    let mine = submit(&services, me(), project, sample(5)).await.unwrap();
    let read = |ctx: RequestContext, probe| {
        services.read_connection_probe(
            ctx.read((ProbeParam { probe }, ProbeWaitParams { wait: None }))
                .in_project(project),
        )
    };
    read(me(), mine.probe_id).await.unwrap();
    let theirs = submit(&services, admin(), project, submit_of(ProbeKind::Check))
        .await
        .unwrap();
    assert_eq!(
        read(me(), theirs.probe_id).await.unwrap_err().api_error(),
        ApiError::NotFound
    );
    services
        .cancel_connection_probe(
            me().update(
                ProbeParam {
                    probe: mine.probe_id,
                },
                (),
            )
            .in_project(project),
        )
        .await
        .unwrap();

    assert_eq!(
        services
            .read_connection_catalog(
                delegated(maintainer)
                    .read(collection_param("crm"))
                    .in_project(project),
            )
            .await
            .unwrap_err()
            .api_error(),
        ApiError::Forbidden
    );
    assert_eq!(
        services
            .list_connections(delegated(maintainer).list((), ListParams::default()))
            .await
            .unwrap()
            .data
            .len,
        0,
        "a delegated token does not carry maintain_catalog"
    );

    let forbidden = |result: Result<(), td_error::TdError>| {
        assert_eq!(result.unwrap_err().api_error(), ApiError::Forbidden);
    };
    forbidden(
        services
            .create_collection(
                me().create(
                    (),
                    de(serde_json::json!({"name": "dfx", "description": "d"})),
                )
                .in_project(project),
            )
            .await
            .map(|_| ()),
    );
    forbidden(
        services
            .execute_function(
                me().create(
                    (
                        collection_param("crm"),
                        de(serde_json::json!({"function": "f"})),
                    ),
                    de(serde_json::json!({})),
                )
                .in_project(project),
            )
            .await
            .map(|_| ()),
    );
    forbidden(
        services
            .table_download(
                me().read((
                    collection_param("crm"),
                    de(serde_json::json!({"table": "t"})),
                    de(serde_json::json!({})),
                    de(serde_json::json!({})),
                ))
                .in_project(project),
            )
            .await
            .map(|_| ()),
    );
    forbidden(
        services
            .table_sample(
                me().read((
                    collection_param("crm"),
                    de(serde_json::json!({"table": "t"})),
                    de(serde_json::json!({})),
                    de(serde_json::json!({})),
                    de(serde_json::json!({})),
                    de(serde_json::json!({})),
                    de(serde_json::json!({})),
                    de(serde_json::json!({})),
                    de(serde_json::json!({})),
                ))
                .in_project(project),
            )
            .await
            .map(|_| ()),
    );
}
