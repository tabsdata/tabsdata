//
// Copyright 2025 Tabs Data Inc.
//

use crate::router::state::Executions;
use crate::routers;
use td_apiforge::apiserver_tag;

pub mod callback;

apiserver_tag!(name = "Execution", description = "Execution API");

routers! {
    state => { Executions },
    router => {
        callback => { state ( Executions ) },
    }
}
