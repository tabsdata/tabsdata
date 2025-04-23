//
// Copyright 2025. Tabs Data Inc.
//

use crate::router::auth::{logout, password_change, refresh_token, role_change, user_info};
use crate::router::AuthState;
use crate::routers;

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
