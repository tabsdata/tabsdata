//
// Copyright 2025. Tabs Data Inc.
//

use crate::bin::apiserver::AuthState;
use crate::routers;

use crate::bin::apiserver::auth::{logout, password_change, refresh_token, role_change, user_info};

routers! {
    state => { AuthState },
    router => {
        password_change => { state ( AuthState ) },
        refresh_token => { state ( AuthState ) },
        user_info => { state ( AuthState ) },
        role_change => { state ( AuthState ) },
        logout => { state ( AuthState ) },
    }
}
