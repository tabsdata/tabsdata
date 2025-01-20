//
// Copyright 2024 Tabs Data Inc.
//

//! Dataset API Service for API Server.

pub mod create;
pub mod delete;
pub mod history;
pub mod list;
pub mod read;
pub mod update;
pub mod upload;

use crate::bin::apisrv::api_server::DatasetsState;
use crate::routers;
use td_utoipa::api_server_tag;

pub const DATASETS: &str = "/collections/{collection}/functions";
pub const DATASET: &str = "/collections/{collection}/functions/{dataset}";

api_server_tag!(name = "Functions", description = "Functions API");

routers! {
    state => { DatasetsState },
    router => {
        create => { state ( DatasetsState ) },
        update => { state ( DatasetsState ) },
        upload => { state ( DatasetsState ) },
        read => { state ( DatasetsState ) },
        list => { state ( DatasetsState ) },
        history => { state ( DatasetsState ) },
        delete => { state ( DatasetsState ) },
    }
}
