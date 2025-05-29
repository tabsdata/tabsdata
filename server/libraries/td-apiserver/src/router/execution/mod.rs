//
//   Copyright 2024 Tabs Data Inc.
//

use crate::router::state::Execution;
use crate::routers;
use td_apiforge::apiserver_tag;

pub mod callback;
pub mod execute;
pub mod read_run;
pub mod synchrotron;

apiserver_tag!(name = "Execution", description = "Execution API");

routers! {
    state => { Execution },
    router => {
        execute => { state ( Execution ) },
        read_run => { state ( Execution ) },
        synchrotron => { state ( Execution ) },
    }
}
