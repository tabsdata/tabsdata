//
// Copyright 2025. Tabs Data Inc.
//

use crate::router::auth::{login, password_change};
use crate::router::AuthState;
use crate::routers;

routers! {
    state => { AuthState },
    router => {
        login => { state ( AuthState ) },
        password_change => { state ( AuthState ) },
    }
}
