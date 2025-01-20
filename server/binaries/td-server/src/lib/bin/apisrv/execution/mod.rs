//
//   Copyright 2024 Tabs Data Inc.
//

use crate::bin::apisrv::api_server::DatasetsState;
use crate::routers;
use getset::Getters;
use serde::Deserialize;
use td_objects::dlo::{DataVersionId, TransactionId};
use td_utoipa::api_server_tag;
use utoipa::IntoParams;

pub mod commits_list;
pub mod execute;
pub mod execution_read;
pub mod executions_list;
pub mod transaction_cancel;
pub mod transaction_recover;
pub mod transactions_list;
pub mod update;
pub mod worker_logs_read;
pub mod worker_messages_list;

api_server_tag!(name = "Execution", description = "Execution API");

routers! {
    state => { DatasetsState },
    router => {
        execute => { state ( DatasetsState ) },
        executions_list => { state ( DatasetsState ) },
        execution_read => { state ( DatasetsState ) },
        transactions_list => { state ( DatasetsState ) },
        transaction_recover => { state ( DatasetsState ) },
        transaction_cancel => { state ( DatasetsState ) },
        commits_list => { state ( DatasetsState ) },
        worker_messages_list => { state ( DatasetsState ) },
        worker_logs_read => { state ( DatasetsState ) },
    }
}

#[derive(Deserialize, Getters, IntoParams)]
#[getset(get = "pub")]
#[allow(dead_code)]
pub struct TransactionUriParams {
    /// Transaction ID
    transaction_id: String,
}

impl From<TransactionUriParams> for TransactionId {
    fn from(params: TransactionUriParams) -> Self {
        TransactionId::new(params.transaction_id)
    }
}

#[derive(Deserialize, Getters, IntoParams)]
#[getset(get = "pub")]
#[allow(dead_code)]
pub struct DataVersionUriParams {
    /// Data Version ID
    data_version_id: String,
}

impl From<DataVersionUriParams> for DataVersionId {
    fn from(params: DataVersionUriParams) -> Self {
        DataVersionId::new(params.data_version_id)
    }
}
