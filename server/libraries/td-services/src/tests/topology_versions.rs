//
// Copyright 2026 Tabsdata Inc.
//

//! A topology version is a graph version: a unit of work opens one iff it
//! writes functions, tables, dependencies or triggers. Project, collection
//! and group writes version their own timelines without one.

use crate::Services;
use crate::collection::services::connection::test_support::create_request;
use crate::tests::fixtures::register;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::TdError;
use td_objects::dxo::collection::{CollectionParam, CollectionUpdate};
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_objects::dxo::group::{GroupCreate, GroupParam, GroupUpdate};
use td_objects::dxo::project::{ProjectCreate, ProjectParam, ProjectUpdate};
use td_objects::types::basic::{
    AccessTokenId, BasePath, CollectionIdName, Description, GroupIdName, GroupName, ProjectId,
    ProjectIdName, ProjectName, UserId,
};

fn admin() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

async fn versions(services: &Services, project: ProjectId) -> Result<usize, TdError> {
    let request = admin().list((), ListParams::default()).in_project(project);
    Ok(services
        .clone()
        .list_topology_versions(request)
        .await?
        .data
        .data
        .len())
}

#[td_test::test(db)]
async fn test_only_graph_writes_open_topology_versions(db: TdPool) -> Result<(), TdError> {
    let services = Services::with_defaults(db.clone());
    let project = services
        .clone()
        .create_project(admin().create(
            (),
            ProjectCreate {
                name: ProjectName::try_from("p")?,
                description: Description::try_from("d")?,
                base_path: BasePath::default(),
                secrets: Vec::new(),
            },
        ))
        .await?
        .data
        .project_id;
    assert_eq!(
        versions(&services, project).await?,
        1,
        "the empty first version"
    );

    services
        .clone()
        .update_project(admin().update(
            ProjectParam {
                project: ProjectIdName::from_str("p")?,
            },
            ProjectUpdate {
                name: Some(ProjectName::try_from("p_renamed")?),
                description: None,
                secrets: None,
            },
        ))
        .await?;
    services
        .clone()
        .create_group(
            admin()
                .create(
                    (),
                    GroupCreate {
                        name: GroupName::try_from("g")?,
                        description: Description::try_from("d")?,
                    },
                )
                .in_project(project),
        )
        .await?;
    services
        .clone()
        .update_group(
            admin()
                .update(
                    GroupParam {
                        group: GroupIdName::from_str("g")?,
                    },
                    GroupUpdate {
                        name: Some(GroupName::try_from("g_renamed")?),
                        description: None,
                    },
                )
                .in_project(project),
        )
        .await?;
    services
        .clone()
        .delete_group(
            admin()
                .delete(GroupParam {
                    group: GroupIdName::from_str("g_renamed")?,
                })
                .in_project(project),
        )
        .await?;
    services
        .clone()
        .create_collection(create_request(project, "c", None))
        .await?;
    services
        .clone()
        .update_collection(
            admin()
                .update(
                    CollectionParam {
                        collection: CollectionIdName::from_str("c")?,
                    },
                    CollectionUpdate {
                        name: Some("c_renamed".try_into()?),
                        description: None,
                        base_path: None,
                    },
                )
                .in_project(project),
        )
        .await?;
    services
        .clone()
        .create_collection(create_request(project, "empty", None))
        .await?;
    services
        .clone()
        .delete_collection(
            admin()
                .delete(CollectionParam {
                    collection: CollectionIdName::from_str("empty")?,
                })
                .in_project(project),
        )
        .await?;
    assert_eq!(
        versions(&services, project).await?,
        1,
        "label writes and an empty collection's delete open no version"
    );

    services
        .clone()
        .register_function(
            admin()
                .create(
                    CollectionParam {
                        collection: CollectionIdName::from_str("c_renamed")?,
                    },
                    register("f", None),
                )
                .in_project(project),
        )
        .await?;
    assert_eq!(
        versions(&services, project).await?,
        2,
        "a register is a graph write"
    );

    services
        .clone()
        .delete_collection(
            admin()
                .delete(CollectionParam {
                    collection: CollectionIdName::from_str("c_renamed")?,
                })
                .in_project(project),
        )
        .await?;
    assert_eq!(
        versions(&services, project).await?,
        3,
        "deleting a collection with functions closes graph rows"
    );
    Ok(())
}
