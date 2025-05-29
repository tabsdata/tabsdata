//
// Copyright 2025. Tabs Data Inc.
//

use crate::router::state::{StorageRef, Tables};
use crate::routers;
use td_apiforge::apiserver_tag;

pub mod download;
pub mod list;
pub mod list_table_data_versions;
pub mod sample;
pub mod schema;

apiserver_tag!(name = "Tables", description = "Tables API");

routers! {
    state => { Tables, StorageRef },
    router => {
        download => { state ( Tables, StorageRef ) },
        list => { state ( Tables ) },
        list_table_data_versions => { state ( Tables ) },
        sample => { state ( Tables ) },
        schema => { state ( Tables ) },
    }
}
