//
// Copyright 2025. Tabs Data Inc.
//

use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ReadRequest, RequestContext};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{
    AuthzOn, CollAdmin, CollDev, CollExec, CollRead, CollReadAll,
};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlSelectIdOrNameService};
use td_objects::types::basic::{CollectionId, CollectionIdName};
use td_objects::types::collection::CollectionDB;
use td_objects::types::table::TableSampleAtName;
use td_storage::Storage;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::service_provider::{ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct BoxedSyncStream(
    pub Pin<Box<dyn Stream<Item = Result<Bytes, TdError>> + Send + Sync + 'static>>,
);

impl BoxedSyncStream {
    pub fn new<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<Bytes, TdError>> + Send + Sync + 'static,
    {
        Self(Box::pin(stream))
    }

    pub fn into_inner(
        self,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes, TdError>> + Send + Sync + 'static>> {
        self.0
    }
}

pub struct TableSampleService {
    provider: ServiceProvider<ReadRequest<TableSampleAtName>, BoxedSyncStream, TdError>,
}

impl TableSampleService {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>, storage: Arc<Storage>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries, authz_context, storage),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, authz_context: Arc<AuthzContext>, storage: Arc<Storage>) {
            service_provider!(layers!(
                ConnectionProvider::new(db),
                SrvCtxProvider::new(queries),
                SrvCtxProvider::new(authz_context),
                SrvCtxProvider::new(storage),

                from_fn(With::<ReadRequest<TableSampleAtName>>::extract::<RequestContext>),
                from_fn(With::<ReadRequest<TableSampleAtName>>::extract_name::<TableSampleAtName>),

                from_fn(With::<TableSampleAtName>::extract::<CollectionIdName>),

                // find collection ID
                from_fn(By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
                from_fn(With::<CollectionDB>::extract::<CollectionId>),

                // check requester has collection permissions
                from_fn(AuthzOn::<CollectionId>::set),
                from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead, CollReadAll>::check),

                //TODO
                //recover get_table_sample` layer from 0.9.6
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<ReadRequest<TableSampleAtName>, BoxedSyncStream, TdError> {
        self.provider.make().await
    }
}
