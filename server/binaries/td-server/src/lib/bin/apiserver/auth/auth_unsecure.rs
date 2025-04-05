//
// Copyright 2025. Tabs Data Inc.
//

use crate::bin::apiserver::AuthState;
use crate::routers;

use crate::bin::apiserver::auth::login;

routers! {
    state => { AuthState },
    router => {
        login => { state ( AuthState ) },
    }
}
