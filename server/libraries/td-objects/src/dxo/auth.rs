//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
mod definitions {
    use crate::types::basic::{
        AccessToken, AccessTokenExpiration, AccessTokenId, AtTime, GrantType, NewPassword,
        OldPassword, Password, RefreshToken, RefreshTokenId, RoleId, RoleName, SessionStatus,
        TokenType, UserId, UserName,
    };

    #[td_type::Dto]
    pub struct Login {
        #[td_type(extractor)]
        pub name: UserName,
        #[td_type(extractor)]
        pub password: Password,
        #[td_type(extractor)]
        #[serde(default = "RoleName::user")]
        pub role: RoleName,
    }

    #[td_type::Dto]
    pub struct RefreshRequestX {
        pub grant_type: GrantType,
        #[td_type(extractor)]
        pub refresh_token: RefreshToken,
    }

    #[td_type::Dto]
    pub struct TokenResponseX {
        pub access_token: AccessToken,
        pub refresh_token: RefreshToken,
        pub token_type: TokenType,
        pub expires_in: AccessTokenExpiration,
    }

    #[td_type::Dto]
    pub struct RoleChange {
        #[td_type(extractor)]
        pub role: RoleName,
    }

    #[td_type::Dto]
    pub struct PasswordChange {
        #[td_type(extractor)]
        pub name: UserName,
        #[td_type(extractor)]
        pub old_password: OldPassword,
        #[td_type(extractor)]
        pub new_password: NewPassword,
    }

    #[td_type::Dao]
    #[dao(sql_table = "sessions")]
    pub struct SessionDB {
        #[builder(default)]
        pub access_token_id: AccessTokenId,
        #[builder(default)]
        pub refresh_token_id: RefreshTokenId,
        #[td_type(setter)]
        pub user_id: UserId,
        #[td_type(setter)]
        pub role_id: RoleId,
        #[td_type(setter)]
        pub created_on: AtTime,
        pub expires_on: AtTime,
        #[builder(default = "AtTime::default()")]
        // TODO, ideally we should support #[td_type(setter)] on the same type multiple times.
        pub status_change_on: AtTime,
        #[builder(default = "SessionStatus::Active")]
        pub status: SessionStatus,
    }

    #[td_type::Dao]
    #[dao(sql_table = "sessions__with_names")]
    #[inherits(SessionDB)]
    pub struct SessionDBWithNames {
        pub user_name: UserName,
        pub role_name: RoleName,
    }

    #[td_type::Dao]
    #[dao(sql_table = "sessions")]
    pub struct SessionLogoutDB {
        #[td_type(setter)]
        pub status_change_on: AtTime,
        #[builder(default = "SessionStatus::InvalidLogout")]
        pub status: SessionStatus,
    }

    #[td_type::Dao]
    #[dao(sql_table = "sessions")]
    pub struct SessionPasswordChangeDB {
        #[td_type(setter)]
        pub status_change_on: AtTime,
        #[builder(default = "SessionStatus::InvalidPasswordChange")]
        pub status: SessionStatus,
    }

    #[td_type::Dao]
    #[dao(sql_table = "sessions")]
    pub struct SessionRoleChangeDB {
        #[td_type(setter)]
        pub status_change_on: AtTime,
        #[builder(default = "SessionStatus::InvalidRoleChange")]
        pub status: SessionStatus,
    }

    #[td_type::Dao]
    #[dao(sql_table = "sessions")]
    pub struct SessionNewTokenDB {
        #[td_type(setter)]
        pub status_change_on: AtTime,
        #[builder(default = "SessionStatus::InvalidNewToken")]
        pub status: SessionStatus,
    }

    #[td_type::Dao]
    #[dao(sql_table = "users_roles__with_names")]
    pub struct UserInfoUserRoleDB {
        pub user_id: UserId,
        pub role_id: RoleId,
        pub role: RoleName,
    }
}
