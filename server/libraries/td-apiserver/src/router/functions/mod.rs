//
// Copyright 2024 Tabs Data Inc.
//

//! Functions API Service for API Server.

pub mod delete;
pub mod read;
pub mod read_version;
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
        register => { state ( Functions ) },
        update => { state ( Functions ) },
        upload => { state ( Functions ) },
        read => { state ( Functions ) },
        read_version => { state ( Functions ) },
        delete => { state ( Functions ) },
    }
}
