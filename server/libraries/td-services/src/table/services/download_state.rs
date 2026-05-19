//
// Copyright 2025. Tabs Data Inc.
//

use crate::table::layers::download::get_table_download;
use crate::table::layers::download_state::get_table_at_id_name;
use crate::table::layers::find_data_version_location_at;
use ta_services::factory::service_factory;
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::rest_urls::internal::FunctionVersionAtId;
use td_objects::rest_urls::params::TableAtIdName;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{AtTime, FunctionVersionId};
use td_objects::types::stream::BoxedSyncStream;
use td_queries::By;
use td_storage::Storage;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = TableStateDownloadService,
    request = ReadRequest<FunctionVersionAtId>,
    response = BoxedSyncStream,
    connection = ConnectionProvider,
    context = Storage,
)]
fn service() {
    layers!(
        // Extract parameters
        from_fn(With::<ReadRequest<FunctionVersionAtId>>::extract_name::<FunctionVersionAtId>),
        from_fn(With::<FunctionVersionAtId>::extract::<AtTime>),
        // get function
        from_fn(With::<FunctionVersionAtId>::extract::<FunctionVersionId>),
        from_fn(
            By::<FunctionVersionId>::historic_fetch_one::<
                { FunctionDBWithNames::Available },
                FunctionDBWithNames,
            >
        ),
        // find system tables
        from_fn(
            By::<FunctionVersionId>::historic_fetch_all::<
                { TableDBWithNames::System },
                TableDBWithNames,
            >
        ),
        from_fn(get_table_at_id_name),
        // Find table data version location.
        find_data_version_location_at::<_, TableAtIdName>(),
        from_fn(get_table_download),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Context;
    use crate::table::services::download::tests::test_download_tables;
    use bytes::Bytes;
    use futures_util::StreamExt;
    use ta_services::factory::ServiceFactory;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::system::TD_FN_STATE_PREFIX;
    use td_objects::types::basic::{AccessTokenId, RoleId};
    use td_objects::types::basic::{
        AtTime, BundleId, CollectionName, Decorator, TableNameDto, UserId,
    };
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_table_download_state(db: TdPool) {
        use crate::table::layers::storage::resolve_table_location;
        use td_objects::dxo::table::TableDBWithNames;

        use td_objects::types::basic::{
            AtTime, CollectionIdName, TableId, TableIdName, TriggeredOn, UncommittedStatus,
        };

        use td_objects::And;
        use td_tower::metadata::type_of_val;

        TableStateDownloadService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ReadRequest<FunctionVersionAtId>, BoxedSyncStream>(&[
                // Extract parameters
                type_of_val(
                    &With::<ReadRequest<FunctionVersionAtId>>::extract_name::<FunctionVersionAtId>,
                ),
                type_of_val(&With::<FunctionVersionAtId>::extract::<AtTime>),
                // get function
                type_of_val(&With::<FunctionVersionAtId>::extract::<FunctionVersionId>),
                type_of_val(
                    &By::<FunctionVersionId>::historic_fetch_one::<
                        { FunctionDBWithNames::Available },
                        FunctionDBWithNames,
                    >,
                ),
                // find system table
                type_of_val(
                    &By::<FunctionVersionId>::historic_fetch_all::<
                        { TableDBWithNames::System },
                        TableDBWithNames,
                    >,
                ),
                type_of_val(&get_table_at_id_name),
                // Find table data version location.
                // Extract parameters
                type_of_val(&With::<TableAtIdName>::extract::<CollectionIdName>),
                type_of_val(&With::<TableAtIdName>::extract::<TableIdName>),
                type_of_val(&With::<TableAtIdName>::extract::<AtTime>),
                type_of_val(&With::<TableAtIdName>::extract::<UncommittedStatus>),
                // Find Table ID, looking at the version at the time
                type_of_val(
                    &By::<And![CollectionIdName, TableIdName]>::historic_fetch_one::<
                        { TableDBWithNames::Available },
                        TableDBWithNames,
                    >,
                ),
                type_of_val(&With::<TableDBWithNames>::extract::<TableId>),
                // At the triggered on time
                type_of_val(&With::<AtTime>::try_convert_to::<TriggeredOn, _>),
                // Find the latest data version of the table ID, at that time
                // Resolve the location of the data version. This takes into account versions without
                // data changes (in which the previous version is resolved)
                type_of_val(&resolve_table_location),
                type_of_val(&get_table_download),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_download_state(db: TdPool) -> Result<(), TdError> {
        let context = Context::with_defaults(db);

        let collection = context
            .db
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let created_tables = vec![TableNameDto::try_from("t0")?];
        let dependencies = None;
        let triggers = None;

        let create = FunctionRegister::builder()
            .try_name("joaquin")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(dependencies)
            .triggers(triggers)
            .tables(Some(created_tables.clone()))
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let function = context.db.seed().function(&collection, &create).await?;
        let system_tables = context.db.seed().system_tables(&function).await?;
        let td_fn_state_table = system_tables
            .iter()
            .find(|t| t.name.starts_with(TD_FN_STATE_PREFIX))
            .unwrap();

        let function_version_id = function.id.clone();

        let get_download = async move |context: &Context,
                                       _table: &str, // not using table because always getting state table of the fn
                                       at_time: &AtTime|
                    -> Result<Bytes, TdError> {
            // with ids
            let request =
                RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                    .read(FunctionVersionAtId {
                        function_version_id: function_version_id.clone(),
                        at: *at_time,
                    });
            let service = TableStateDownloadService::build(context);
            let response = service.service().raw_oneshot(request).await?;
            let mut stream = response.into_inner();
            let bytes_with_ids = match stream.next().await {
                Some(res) => res?,
                None => Bytes::new(),
            };
            // with names
            let request =
                RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                    .read(FunctionVersionAtId {
                        function_version_id: function_version_id.clone(),
                        at: *at_time,
                    });
            let service = TableStateDownloadService::build(context);
            let response = service.service().raw_oneshot(request).await?;
            let mut stream = response.into_inner();
            let bytes_with_names = match stream.next().await {
                Some(res) => res?,
                None => Bytes::new(),
            };
            assert_eq!(bytes_with_ids, bytes_with_names);
            Ok(bytes_with_ids)
        };

        let tables = std::slice::from_ref(&td_fn_state_table.name);
        test_download_tables(&context, &collection, &function, tables, get_download).await
    }
}
