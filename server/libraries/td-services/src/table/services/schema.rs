//
// Copyright 2025. Tabs Data Inc.
//

use crate::table::layers::schema::{get_table_schema, resolve_table_location};
use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ReadRequest, RequestContext};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{
    AuthzOn, CollAdmin, CollDev, CollExec, CollRead, CollReadAll,
};
use td_objects::tower_service::from::{combine, ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlSelectIdOrNameService};
use td_objects::types::basic::{CollectionId, CollectionIdName, FunctionVersionId, TableIdName};
use td_objects::types::collection::CollectionDB;
use td_objects::types::execution::TableDataVersionDB;
use td_objects::types::table::{TableAtName, TableSchema};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::service_provider::{ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct TableSchemaService {
    provider: ServiceProvider<ReadRequest<TableAtName>, TableSchema, TdError>,
}

impl TableSchemaService {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries, authz_context),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, authz_context: Arc<AuthzContext>) {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                ConnectionProvider::new(db),
                SrvCtxProvider::new(authz_context),

                from_fn(With::<ReadRequest<TableAtName>>::extract::<RequestContext>),
                from_fn(With::<ReadRequest<TableAtName>>::extract_name::<TableAtName>),

                from_fn(With::<TableAtName>::extract::<CollectionIdName>),

                // find collection ID
                from_fn(By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
                from_fn(With::<CollectionDB>::extract::<CollectionId>),

                // check requester has collection permissions
                from_fn(AuthzOn::<CollectionId>::set),
                from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead, CollReadAll>::check),

                // find table data version
                // TODO At is not yet used (using current time for now)
                from_fn(With::<TableAtName>::extract::<TableIdName>),
                from_fn(combine::<CollectionIdName, TableIdName>),
                from_fn(By::<(CollectionIdName, TableIdName)>::select::<DaoQueries, TableDataVersionDB>),
                from_fn(With::<TableDataVersionDB>::extract::<FunctionVersionId>),

                // find function version
                // from_fn(By::<FunctionVersionId>::select::<DaoQueries, FunctionVersionDB>),

                // get schema
                // TODO storage is missing, we need it in the service
                from_fn(resolve_table_location),
                from_fn(get_table_schema),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<ReadRequest<TableAtName>, TableSchema, TdError> {
        self.provider.make().await
    }
}
