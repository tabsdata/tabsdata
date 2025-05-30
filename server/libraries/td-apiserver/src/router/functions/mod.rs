//
// Copyright 2024 Tabs Data Inc.
//

//! Functions API Service for API Server.

pub mod delete;
pub mod list;
pub mod read;
pub mod register;
pub mod update;
pub mod upload;

use crate::router::state::Functions;
use crate::routers;
use td_apiforge::apiserver_tag;

apiserver_tag!(name = "Functions", description = "Functions API");

routers! {
    state => { Functions },
    router => {
        delete => { state ( Functions ) },
        list => { state ( Functions ) },
        read => { state ( Functions ) },
        register => { state ( Functions ) },
        update => { state ( Functions ) },
        upload => { state ( Functions ) },
    }
}
