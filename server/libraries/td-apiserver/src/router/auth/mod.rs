//
// Copyright 2025. Tabs Data Inc.
//

use td_apiforge::apiserver_tag;

pub mod login;
pub mod logout;
pub mod password_change;
pub mod role_change;
pub mod user_info;

pub mod auth_secure;
pub mod auth_unsecure;
pub mod authorization_layer;
pub mod cert_download;
pub mod refresh_token;

apiserver_tag!(name = "Auth", description = "Authentication API");
