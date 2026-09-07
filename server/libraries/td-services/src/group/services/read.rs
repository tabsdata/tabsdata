//
// Copyright 2026 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::crudl::{ReadRequest, WithinProject};
use td_objects::dxo::group::{Group, GroupDBWithNames, GroupParam};
use td_queries::Queries;

#[service_factory]
async fn read_group(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ReadRequest<GroupParam>>,
) -> Result<Group, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, param) = request.into_parts();
    // List + read are open to any role with access to the project
    // (groups are referenced by name across the API).
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);
    let with_names: GroupDBWithNames = conn.fetch_one(&param.group).await?;
    Group::from_dao(with_names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::group::GroupDB;
    use td_objects::types::basic::{AccessTokenId, Description, GroupIdName, GroupName, UserId};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_read_group_by_name(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let _ = db
            .scoped(p.id)
            .seed()
            .group(
                GroupName::try_from("analytics")?,
                Description::try_from("Analytics tier group")?,
            )
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .read(GroupParam {
                group: GroupIdName::from_str("analytics")?,
            })
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.read_group(request).await?.data;
        let found = db
            .fetch_one::<GroupDB>(&GroupName::try_from("analytics")?)
            .await?;
        assert_eq!(response.id, found.base.id);
        assert_eq!(response.name, found.base.name);
        assert_eq!(response.description, found.base.description);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_read_system_group_by_name(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .read(GroupParam {
                group: GroupIdName::from_str("default")?,
            })
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.read_group(request).await?.data;
        assert_eq!(response.name, GroupName::default_group());
        Ok(())
    }
}
