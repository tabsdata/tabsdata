//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::types::basic::{
        AccessToken, AccessTokenExpiration, AccessTokenId, AtTime, GrantType, NewPassword,
        OldPassword, Password, RefreshToken, RefreshTokenId, RoleId, RoleName, SessionStatus,
        TokenType, UserId, UserName,
    };

    #[ta_objects::Dto]
    pub struct Login {
        #[dxo(extractor)]
        pub name: UserName,
        #[dxo(extractor)]
        pub password: Password,
        #[dxo(extractor)]
        #[serde(default = "RoleName::user")]
        pub role: RoleName,
    }

    #[ta_objects::Dto]
    pub struct RefreshRequestX {
        pub grant_type: GrantType,
        #[dxo(extractor)]
        pub refresh_token: RefreshToken,
    }

    #[ta_objects::Dto]
    pub struct TokenResponseX {
        pub access_token: AccessToken,
        pub refresh_token: RefreshToken,
        pub token_type: TokenType,
        pub expires_in: AccessTokenExpiration,
    }

    #[ta_objects::Dto]
    pub struct RoleChange {
        #[dxo(extractor)]
        pub role: RoleName,
    }

    #[ta_objects::Dto]
    pub struct PasswordChange {
        #[dxo(extractor)]
        pub name: UserName,
        #[dxo(extractor)]
        pub old_password: OldPassword,
        #[dxo(extractor)]
        pub new_password: NewPassword,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "sessions")]
    pub struct SessionDB {
        #[builder(default)]
        pub access_token_id: AccessTokenId,
        #[builder(default)]
        pub refresh_token_id: RefreshTokenId,
        #[dxo(setter)]
        pub user_id: UserId,
        #[dxo(setter)]
        pub role_id: RoleId,
        #[dxo(setter)]
        pub created_on: AtTime,
        pub expires_on: AtTime,
        #[builder(default = "AtTime::default()")]
        // TODO, ideally we should support #[dxo(setter)] on the same type multiple times.
        pub status_change_on: AtTime,
        #[builder(default = "SessionStatus::Active")]
        pub status: SessionStatus,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "sessions__with_names")]
    #[inherits(SessionDB)]
    pub struct SessionDBWithNames {
        pub user_name: UserName,
        pub role_name: RoleName,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "sessions")]
    pub struct SessionLogoutDB {
        #[dxo(setter)]
        pub status_change_on: AtTime,
        #[builder(default = "SessionStatus::InvalidLogout")]
        pub status: SessionStatus,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "sessions")]
    pub struct SessionPasswordChangeDB {
        #[dxo(setter)]
        pub status_change_on: AtTime,
        #[builder(default = "SessionStatus::InvalidPasswordChange")]
        pub status: SessionStatus,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "sessions")]
    pub struct SessionRoleChangeDB {
        #[dxo(setter)]
        pub status_change_on: AtTime,
        #[builder(default = "SessionStatus::InvalidRoleChange")]
        pub status: SessionStatus,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "sessions")]
    pub struct SessionNewTokenDB {
        #[dxo(setter)]
        pub status_change_on: AtTime,
        #[builder(default = "SessionStatus::InvalidNewToken")]
        pub status: SessionStatus,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "users_roles__with_names")]
    pub struct UserInfoUserRoleDB {
        pub user_id: UserId,
        pub role_id: RoleId,
        pub role: RoleName,
    }
}

// Auth requests operate on global identity tables (users/sessions), not
// project-scoped data; their services declare `scope = Global`.
