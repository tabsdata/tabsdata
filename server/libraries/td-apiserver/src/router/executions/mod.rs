//
// Copyright 2025 Tabs Data Inc.
//

use crate::router::state::Executions;
use crate::routers;
use td_apiforge::apiserver_tag;

pub mod cancel;
pub mod execute;
pub mod list;
pub mod recover;

apiserver_tag!(name = "Execution", description = "Execution API");

routers! {
    state => { Executions },
    router => {
        cancel => { state ( Executions ) },
        execute => { state ( Executions ) },
        recover => { state ( Executions ) },
    }
}
