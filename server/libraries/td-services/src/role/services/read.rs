//
// Copyright 2025 Tabs Data Inc.
//

use crate::common::layers::sql::select_by_id_or_name;
use crate::common::layers::{build, try_from};
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::ReadRequest;
use td_objects::sql::roles::RoleQueries;
use td_objects::tower_service::extractor::{extract_req_context, extract_req_name};
use td_objects::types::role::{Role, RoleBuilder, RoleDBWithNames, RoleParam};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ReadRoleService {
    provider: ServiceProvider<ReadRequest<RoleParam>, Role, TdError>,
}

impl ReadRoleService {
    pub fn new(db: DbPool) -> Self {
        let queries = Arc::new(RoleQueries::new());
        Self {
            provider: Self::provider(db, queries),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<RoleQueries>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                from_fn(extract_req_context::<ReadRequest<RoleParam >>),
                from_fn(extract_req_name::<ReadRequest<RoleParam>, _>),

                ConnectionProvider::new(db),
                from_fn(select_by_id_or_name::<RoleQueries, RoleParam, _, _, RoleDBWithNames>),

                from_fn(try_from::<RoleDBWithNames, RoleBuilder>),
                from_fn(build::<RoleBuilder, Role>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<ReadRequest<RoleParam>, Role, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_common::id::Id;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_user::admin_user;
    use td_objects::types::basic::{Description, Fixed, RoleId, RoleName};
    use td_security::ENCODED_ID_ROLE_SYS_ADMIN;
    use td_tower::ctx_service::RawOneshot;

    #[tokio::test]
    async fn test() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let admin_id = admin_user(&db).await;

        let sys_admin_id = Id::try_from(ENCODED_ID_ROLE_SYS_ADMIN)?;

        // With name
        let request = RequestContext::with(&admin_id, "r", true)
            .await
            .read(RoleParam::try_from("sys_admin")?);

        let service = ReadRoleService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        assert_eq!(*response.id(), RoleId::from(sys_admin_id));
        assert_eq!(*response.name(), RoleName::try_from("sys_admin")?);
        assert_eq!(
            *response.description(),
            Description::try_from("System Administrator Role")?
        );
        assert_eq!(*response.fixed(), Fixed::try_from(true)?);

        // With id
        let request = RequestContext::with(&admin_id, "r", true)
            .await
            .read(RoleParam::try_from(format!("~{}", sys_admin_id))?);

        let service = ReadRoleService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        assert_eq!(*response.id(), RoleId::from(sys_admin_id));
        assert_eq!(*response.name(), RoleName::try_from("sys_admin")?);
        assert_eq!(
            *response.description(),
            Description::try_from("System Administrator Role")?
        );
        assert_eq!(*response.fixed(), Fixed::try_from(true)?);
        Ok(())
    }
}
