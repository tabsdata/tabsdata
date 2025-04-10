//
// Copyright 2024 Tabs Data Inc.
//

//! Function API Service for API Server.

pub mod execute;

use crate::bin::apiserver::ExecutionState;
use crate::routers;
use td_apiforge::apiserver_tag;

apiserver_tag!(name = "Functions", description = "Functions API");

routers! {
    state => { ExecutionState },
    router => {
        execute => { state ( FunctionsState ) },
    }
}
