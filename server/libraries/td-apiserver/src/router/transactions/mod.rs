//
// Copyright 2025 Tabs Data Inc.
//

use crate::router::state::Transactions;
use crate::routers;

pub mod cancel;
pub mod list;
pub mod recover;
pub mod synchrotron;

routers! {
    state => { Transactions },
    router => {
        cancel => { state ( Transactions ) },
        list => { state ( Transactions ) },
        recover => { state ( Transactions ) },
        synchrotron => { state ( Transactions ) },
    }
}
