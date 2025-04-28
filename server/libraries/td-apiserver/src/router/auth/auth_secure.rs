//
// Copyright 2025. Tabs Data Inc.
//

use crate::router::auth::{logout, refresh_token, role_change, user_info};
use crate::router::AuthState;
use crate::routers;

routers! {
    state => { AuthState },
    router => {
        refresh_token => { state ( AuthState ) },
        user_info => { state ( AuthState ) },
        role_change => { state ( AuthState ) },
        logout => { state ( AuthState ) },
    }
}
