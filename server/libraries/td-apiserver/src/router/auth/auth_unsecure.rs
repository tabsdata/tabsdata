//
// Copyright 2025. Tabs Data Inc.
//

use crate::router::auth::{login, password_change};
use crate::router::state::Auth;
use crate::routers;

routers! {
    state => { Auth },
    router => {
        login => { state ( Auth ) },
        password_change => { state ( Auth ) },
    }
}
