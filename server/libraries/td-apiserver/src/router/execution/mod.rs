//
//   Copyright 2024 Tabs Data Inc.
//

use crate::router::state::Execution;
use crate::routers;
use td_apiforge::apiserver_tag;

pub mod callback;
pub mod cancel_execution;
pub mod cancel_transaction;
pub mod execute;
pub mod read_run;
pub mod recover_execution;
pub mod recover_transaction;
pub mod synchrotron;

apiserver_tag!(name = "Execution", description = "Execution API");

routers! {
    state => { Execution },
    router => {
        cancel_execution => { state ( Execution ) },
        cancel_transaction => { state ( Execution ) },
        execute => { state ( Execution ) },
        read_run => { state ( Execution ) },
        recover_execution => { state ( Execution ) },
        recover_transaction => { state ( Execution ) },
        synchrotron => { state ( Execution ) },
    }
}
