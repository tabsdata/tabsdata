//
// Copyright 2026 Tabsdata Inc.
//

use crate::Services;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::project::{ProjectCreate, ProjectParam, ProjectRead, ProjectUpdate};
use td_objects::dxo::secret::{SecretEntry, SecretUpsert};
use td_objects::types::basic::{
    AccessTokenId, BasePath, Description, ProjectIdName, ProjectName, SecretKey, SecretName, UserId,
};

fn admin_ctx() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

fn project_param(name: &str) -> ProjectParam {
    ProjectParam {
        project: ProjectIdName::Name(ProjectName::try_from(name).unwrap()),
    }
}

fn secret(name: &str, entries: &[(&str, Option<&str>)]) -> SecretUpsert {
    SecretUpsert {
        name: SecretName::try_from(name).unwrap(),
        entries: entries
            .iter()
            .map(|(key, value)| SecretEntry {
                key: SecretKey::try_from(*key).unwrap(),
                value: value.map(str::to_owned),
            })
            .collect(),
    }
}

fn update(secrets: Option<Vec<SecretUpsert>>) -> ProjectUpdate {
    ProjectUpdate {
        name: None,
        description: None,
        secrets,
    }
}

async fn create(
    services: &Services,
    name: &str,
    secrets: Vec<SecretUpsert>,
) -> Result<ProjectRead, TdError> {
    let create = ProjectCreate {
        name: ProjectName::try_from(name)?,
        description: Description::default(),
        base_path: BasePath::default(),
        secrets,
    };
    Ok(services
        .create_project(admin_ctx().create((), create))
        .await?
        .data)
}

async fn keys(services: &Services, project: &ProjectRead) -> Result<Vec<String>, TdError> {
    let read = services
        .read_project_secrets(admin_ctx().read(()).in_project(project.id))
        .await?
        .data;
    Ok(read
        .secrets
        .iter()
        .flat_map(|secret| {
            secret
                .keys
                .iter()
                .map(move |key| format!("{}.{key}", secret.name))
        })
        .collect())
}

async fn values(
    services: &Services,
    project: &ProjectRead,
) -> Result<Vec<(String, Option<String>)>, TdError> {
    let hydrated = services
        .bridge
        .project_secrets()
        .fetch_hydrated(&project.id)
        .await?;
    Ok(hydrated
        .iter()
        .flat_map(|secret| {
            secret.entries.iter().map(move |entry| {
                (
                    format!("{}.{}", secret.name, entry.key),
                    entry.value.clone(),
                )
            })
        })
        .collect())
}

#[td_test::test(db)]
async fn test_create_project_stores_its_secrets(db: TdPool) -> Result<(), TdError> {
    let services = Services::with_defaults(db);
    let project = create(
        &services,
        "payments",
        vec![secret(
            "warehouse",
            &[("user", Some("admin")), ("password", Some("p"))],
        )],
    )
    .await?;
    assert_eq!(
        keys(&services, &project).await?,
        ["warehouse.user", "warehouse.password"]
    );
    assert_eq!(
        values(&services, &project).await?,
        [
            ("warehouse.user".to_owned(), Some("admin".to_owned())),
            ("warehouse.password".to_owned(), Some("p".to_owned())),
        ]
    );
    Ok(())
}

#[td_test::test(db)]
async fn test_create_project_without_secrets_has_none(db: TdPool) -> Result<(), TdError> {
    let services = Services::with_defaults(db);
    let project = create(&services, "payments", Vec::new()).await?;
    assert!(keys(&services, &project).await?.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn test_create_project_cannot_keep_values_it_never_had(db: TdPool) -> Result<(), TdError> {
    let services = Services::with_defaults(db);
    let refused = create(
        &services,
        "payments",
        vec![secret("warehouse", &[("password", None)])],
    )
    .await;
    assert!(refused.is_err());
    assert!(
        services
            .read_project(admin_ctx().read(project_param("payments")))
            .await
            .is_err()
    );
    Ok(())
}

#[td_test::test(db)]
async fn test_update_project_replaces_its_secrets_keeping_values(
    db: TdPool,
) -> Result<(), TdError> {
    let services = Services::with_defaults(db);
    let project = create(
        &services,
        "payments",
        vec![secret(
            "warehouse",
            &[("user", Some("admin")), ("password", Some("p"))],
        )],
    )
    .await?;
    services
        .update_project(admin_ctx().update(
            project_param("payments"),
            update(Some(vec![secret(
                "warehouse",
                &[("password", None), ("token", Some("t"))],
            )])),
        ))
        .await?;
    assert_eq!(
        values(&services, &project).await?,
        [
            ("warehouse.password".to_owned(), Some("p".to_owned())),
            ("warehouse.token".to_owned(), Some("t".to_owned())),
        ]
    );
    Ok(())
}

#[td_test::test(db)]
async fn test_update_project_without_secrets_leaves_them(db: TdPool) -> Result<(), TdError> {
    let services = Services::with_defaults(db);
    let project = create(
        &services,
        "payments",
        vec![secret("warehouse", &[("password", Some("p"))])],
    )
    .await?;
    services
        .update_project(admin_ctx().update(
            project_param("payments"),
            ProjectUpdate {
                description: Some(Description::try_from("Payments")?),
                ..update(None)
            },
        ))
        .await?;
    assert_eq!(
        values(&services, &project).await?,
        [("warehouse.password".to_owned(), Some("p".to_owned()))]
    );
    Ok(())
}

#[td_test::test(db)]
async fn test_update_project_can_clear_its_secrets(db: TdPool) -> Result<(), TdError> {
    let services = Services::with_defaults(db);
    let project = create(
        &services,
        "payments",
        vec![secret("warehouse", &[("password", Some("p"))])],
    )
    .await?;
    services
        .update_project(admin_ctx().update(project_param("payments"), update(Some(Vec::new()))))
        .await?;
    assert!(keys(&services, &project).await?.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn test_rename_project_keeps_its_secrets(db: TdPool) -> Result<(), TdError> {
    let services = Services::with_defaults(db);
    let project = create(
        &services,
        "payments",
        vec![secret("warehouse", &[("password", Some("p"))])],
    )
    .await?;
    services
        .update_project(admin_ctx().update(
            project_param("payments"),
            ProjectUpdate {
                name: Some(ProjectName::try_from("treasury")?),
                ..update(None)
            },
        ))
        .await?;
    assert_eq!(
        values(&services, &project).await?,
        [("warehouse.password".to_owned(), Some("p".to_owned()))]
    );
    Ok(())
}
