//
// Copyright 2025 Tabsdata Inc.
//

use crate::table::download::get_table_download;
use crate::table::download_state::get_table_at_id_name;
use crate::table::find_data_version_location_at;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_database::access::{AcrossProjects, DbPool};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::collection::ResolvedCollectionDB;
use td_objects::dxo::crudl::{AtTimeParam, ReadRequest};
use td_objects::dxo::function::{FunctionDBWithNames, FunctionVersionIdParam};
use td_objects::dxo::table::TableDBWithNames;
use td_objects::types::stream::BoxedSyncStream;
use td_queries::Queries;
use td_storage::Storage;

#[service_factory]
async fn table_state_download(
    authz: Ctx<AuthzContext>,
    storage: Ctx<Storage>,
    db: DbPool<AcrossProjects>,
    request: ReadRequest<(FunctionVersionIdParam, AtTimeParam)>,
) -> Result<BoxedSyncStream, TdError> {
    let (_, (version, at)) = request.into_parts();
    let at = at.at;
    let function_version_id = version.function_version_id;

    let mut trx = db.read().await?;
    let conn = trx.get(authz.none());
    let function: FunctionDBWithNames = conn.as_of(&at).fetch_one(&function_version_id).await?;
    let collection: ResolvedCollectionDB =
        conn.fetch_one(&function.base.base.collection_id).await?;
    let mut trx = trx.enter_project(&collection.base.project_id).await?;
    let conn = trx.get(authz.none());

    let system_tables: Vec<TableDBWithNames> = conn
        .as_of(&at)
        .fetch_all(and!(&function.base.base.id, TableDBWithNames::system()))
        .await?;
    let (collection, table, version_at, uncommitted) =
        get_table_at_id_name(&at, &function, &system_tables).await?;

    let (_, location) = find_data_version_location_at(
        conn,
        &collection.collection,
        &table.table,
        &version_at.at,
        &uncommitted.uncommitted,
    )
    .await?;
    get_table_download(&storage, &location).await
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use crate::table::services::download::tests::test_download_tables;
    use bytes::Bytes;
    use futures_util::StreamExt;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::{AtTimeParam, RequestContext};
    use td_objects::dxo::function::{FunctionRegister, FunctionVersionIdParam};
    use td_objects::system::TD_FN_STATE_PREFIX;
    use td_objects::types::basic::{AccessTokenId, ReuseFrozen};
    use td_objects::types::basic::{
        AtTime, BundleId, CollectionName, Decorator, TableNameDto, UserId,
    };
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db, flavor = "multi_thread", worker_threads = 1)]
    async fn test_download_state(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let context = Services::with_defaults(db.clone());

        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let created_tables = vec![TableNameDto::try_from("t0")?];
        let dependencies = None;
        let triggers = None;

        let create = FunctionRegister {
            name: "joaquin".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies,
            triggers,
            tables: Some(created_tables.clone()),
            table_types: None,
            runtime_values: "mock runtime values".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let system_tables = db.scoped(p.id).seed().system_tables(&function).await?;
        let td_fn_state_table = system_tables
            .iter()
            .find(|t| t.base.name.starts_with(TD_FN_STATE_PREFIX))
            .unwrap();

        let function_version_id = function.base.id.clone();

        let get_download = async move |context: &Services,
                                       _table: &str, // not using table because always getting state table of the fn
                                       at_time: &AtTime|
                    -> Result<Bytes, TdError> {
            let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).read((
                FunctionVersionIdParam {
                    function_version_id: function_version_id.clone(),
                },
                AtTimeParam { at: *at_time },
            ));
            let service = context.clone();
            let response = service.table_state_download(request).await?.data;
            let mut stream = response.into_inner();
            let bytes_with_ids = match stream.next().await {
                Some(res) => res?,
                None => Bytes::new(),
            };
            let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).read((
                FunctionVersionIdParam {
                    function_version_id: function_version_id.clone(),
                },
                AtTimeParam { at: *at_time },
            ));
            let service = context.clone();
            let response = service.table_state_download(request).await?.data;
            let mut stream = response.into_inner();
            let bytes_with_names = match stream.next().await {
                Some(res) => res?,
                None => Bytes::new(),
            };
            assert_eq!(bytes_with_ids, bytes_with_names);
            Ok(bytes_with_ids)
        };

        let tables = std::slice::from_ref(&td_fn_state_table.base.name);
        test_download_tables(
            &db,
            &context,
            &p,
            &collection,
            &function,
            tables,
            get_download,
        )
        .await
    }
}
