//
// Copyright 2025. Tabs Data Inc.
//

use crate::bin::apiserver::AuthState;
use crate::routers;

use crate::bin::apiserver::auth::{login, password_change};

routers! {
    state => { AuthState },
    router => {
        login => { state ( AuthState ) },
        password_change => { state ( AuthState ) },
    }
}
