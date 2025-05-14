//
// Copyright 2025. Tabs Data Inc.
//

use crate::types::basic::{
    AccessToken, AccessTokenExpiration, AccessTokenId, AtTime, Email, FullName, GrantType,
    NewPassword, OldPassword, Password, PasswordMustChange, RefreshToken, RefreshTokenId, RoleId,
    RoleName, SessionStatus, TokenType, UserEnabled, UserId, UserName,
};
use crate::types::permission::Permission;
use crate::types::user::UserDBWithNames;

#[td_type::Dto]
pub struct Login {
    #[td_type(extractor)]
    name: UserName,
    #[td_type(extractor)]
    password: Password,
    #[td_type(extractor)]
    #[serde(default = "RoleName::user")]
    role: RoleName,
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
    name: UserName,
    #[td_type(extractor)]
    old_password: OldPassword,
    #[td_type(extractor)]
    new_password: NewPassword,
}

#[td_type::Dao(sql_table = "sessions")]
#[td_type(builder(try_from = SessionDB))]
pub struct SessionDB {
    #[builder(default)]
    access_token_id: AccessTokenId,
    #[builder(default)]
    refresh_token_id: RefreshTokenId,
    #[td_type(setter)]
    user_id: UserId,
    #[td_type(setter)]
    role_id: RoleId,
    #[td_type(setter)]
    created_on: AtTime,
    expires_on: AtTime,
    #[builder(default = "AtTime::default()")]
    // TODO, ideally we should support #[td_type(setter)] on the same type multiple times.
    status_change_on: AtTime,
    #[builder(default = "SessionStatus::Active")]
    status: SessionStatus,
}

#[td_type::Dao(sql_table = "sessions")]
pub struct SessionLogoutDB {
    #[td_type(setter)]
    status_change_on: AtTime,
    #[builder(default = "SessionStatus::InvalidLogout")]
    status: SessionStatus,
}

#[td_type::Dao(sql_table = "sessions")]
pub struct SessionPasswordChangeDB {
    #[td_type(setter)]
    status_change_on: AtTime,
    #[builder(default = "SessionStatus::InvalidPasswordChange")]
    status: SessionStatus,
}

#[td_type::Dao(sql_table = "sessions")]
pub struct SessionRoleChangeDB {
    #[td_type(setter)]
    status_change_on: AtTime,
    #[builder(default = "SessionStatus::InvalidRoleChange")]
    status: SessionStatus,
}

#[td_type::Dao(sql_table = "sessions")]
pub struct SessionNewTokenDB {
    #[td_type(setter)]
    status_change_on: AtTime,
    #[builder(default = "SessionStatus::InvalidNewToken")]
    status: SessionStatus,
}

#[td_type::Dto]
#[td_type(builder(try_from = UserDBWithNames))]
pub struct UserInfo {
    id: UserId,
    name: UserName,
    full_name: FullName,
    email: Option<Email>,
    created_on: AtTime,
    created_by_id: UserId,
    created_by: UserName,
    modified_on: AtTime,
    modified_by_id: UserId,
    modified_by: UserName,
    password_set_on: AtTime,
    password_must_change: PasswordMustChange,
    enabled: UserEnabled,
    #[td_type(builder(skip), setter)]
    current_role_id: RoleId,
    #[td_type(builder(skip), setter)]
    current_permissions: Vec<Permission>,
    #[td_type(builder(skip), setter)]
    user_roles: Vec<UserInfoRoleIdName>,
}

#[td_type::Dao(sql_table = "users_roles__with_names")]
pub struct UserInfoUserRoleDB {
    user_id: UserId,
    role_id: RoleId,
    role: RoleName,
}

//NOTE: we cannot use RoleIdName as there is already one in the API.
//      For OpenAPI 'there can be only one'
#[td_type::Dto]
#[td_type(builder(try_from = UserInfoUserRoleDB))]
pub struct UserInfoRoleIdName {
    #[td_type(builder(field = "role_id"))]
    id: RoleId,
    #[td_type(builder(field = "role"))]
    name: RoleName,
}
