//
// Copyright 2024 Tabs Data Inc.
//

//! Functions API Service for API Server.

pub mod delete;
pub mod history;
pub mod list_by_collection;
pub mod read;
pub mod register;
pub mod update;
pub mod upload;
pub mod list;

use crate::router::state::Functions;
use crate::routers;
use td_apiforge::apiserver_tag;

apiserver_tag!(name = "Functions", description = "Functions API");

routers! {
    state => { Functions },
    router => {
        delete => { state ( Functions ) },
        list_by_collection => { state ( Functions ) },
        list => { state ( Functions ) },
        read => { state ( Functions ) },
        register => { state ( Functions ) },
        update => { state ( Functions ) },
        upload => { state ( Functions ) },
        history => { state ( Functions ) },
    }
}
