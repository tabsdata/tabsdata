//
// Copyright 2025. Tabs Data Inc.
//

use crate::types::basic::{
    AccessToken, AccessTokenExpiration, AccessTokenId, AtTime, GrantType, NewPassword, OldPassword,
    Password, RefreshToken, RefreshTokenId, RoleId, RoleName, SessionStatus, TokenType, UserId,
    UserName,
};

#[td_type::Dto]
pub struct Login {
    #[td_type(extractor)]
    name: UserName,
    #[td_type(extractor)]
    password: Password,
    #[td_type(extractor)]
    role: Option<RoleName>,
}

#[td_type::Dto]
pub struct RefreshRequestX {
    grant_type: GrantType,
    #[td_type(extractor)]
    refresh_token: RefreshToken,
}

#[td_type::Dto]
pub struct TokenResponseX {
    access_token: AccessToken,
    refresh_token: RefreshToken,
    token_type: TokenType,
    expires_in: AccessTokenExpiration,
}

#[td_type::Dto]
pub struct RoleChange {
    #[td_type(extractor)]
    role: RoleName,
}

#[td_type::Dto]
pub struct PasswordChange {
    #[td_type(extractor)]
    old_password: OldPassword,
    #[td_type(extractor)]
    new_password: NewPassword,
}

#[td_type::Dao(sql_table = "sessions")]
pub struct SessionDB {
    #[builder(default)]
    access_token: AccessTokenId,
    #[builder(default)]
    refresh_token: RefreshTokenId,
    user_id: UserId,
    role_id: RoleId,
    created_on: AtTime,
    expires_on: AtTime,
    status_change_on: AtTime,
    status: SessionStatus,
}

#[td_type::Dto]
pub struct UserInfo {
    user_id: UserId,
    user_name: UserName,
    role_id: RoleId,
    role_name: RoleName,
    roles: Vec<RoleName>,
}
