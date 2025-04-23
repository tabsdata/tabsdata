//
// Copyright 2025. Tabs Data Inc.
//

use crate::router::auth::login;
use crate::router::AuthState;
use crate::routers;

routers! {
    state => { AuthState },
    router => {
        login => { state ( AuthState ) },
    }
}
