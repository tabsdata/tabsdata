//
// Copyright 2025 Tabsdata Inc.
//

use crate::types::basic::{
    AccessToken, AccessTokenExpiration, AccessTokenId, AtTime, Created, Expires, GrantType,
    NewPassword, OldPassword, Password, RefreshToken, RefreshTokenId, SessionStatus, StatusChange,
    TokenType, UserId, UserName,
};

#[ta_objects::Dto]
pub struct Login {
    pub name: UserName,
    pub password: Password,
}

#[ta_objects::Dto]
pub struct RefreshRequest {
    pub grant_type: GrantType,
    pub refresh_token: RefreshToken,
}

#[ta_objects::Dto]
pub struct TokenResponse {
    pub access_token: AccessToken,
    pub refresh_token: RefreshToken,
    pub token_type: TokenType,
    pub expires_in: AccessTokenExpiration,
}

#[ta_objects::Dto]
pub struct PasswordChange {
    pub name: UserName,
    pub old_password: OldPassword,
    pub new_password: NewPassword,
}

#[ta_objects::Dao]
#[dao(sql_table = "sessions")]
pub struct SessionDB {
    pub access_token_id: AccessTokenId,
    pub refresh_token_id: RefreshTokenId,
    pub user_id: UserId,
    pub created_on: Created<AtTime>,
    pub expires_on: Expires<AtTime>,
    pub status_change_on: StatusChange<AtTime>,
    pub status: SessionStatus,
}

impl SessionDB {
    /// A fresh Active session with new token ids.
    pub fn new(user_id: UserId, at: AtTime, expires_on: AtTime) -> Self {
        Self {
            access_token_id: AccessTokenId::default(),
            refresh_token_id: RefreshTokenId::default(),
            user_id,
            created_on: at.into(),
            expires_on: expires_on.into(),
            status_change_on: AtTime::default().into(),
            status: SessionStatus::Active,
        }
    }
}

#[ta_objects::Dao]
#[dao(sql_table = "sessions__with_names")]
pub struct SessionDBWithNames {
    #[dao(flatten)]
    pub base: SessionDB,
    pub user_name: UserName,
}

#[ta_objects::Dao]
#[dao(sql_table = "sessions")]
pub struct SessionLogoutDB {
    pub status_change_on: StatusChange<AtTime>,
    pub status: SessionStatus,
}

impl SessionLogoutDB {
    pub fn new(at: AtTime) -> Self {
        Self {
            status_change_on: at.into(),
            status: SessionStatus::InvalidLogout,
        }
    }
}

#[ta_objects::Dao]
#[dao(sql_table = "sessions")]
pub struct SessionPasswordChangeDB {
    pub status_change_on: StatusChange<AtTime>,
    pub status: SessionStatus,
}

impl SessionPasswordChangeDB {
    pub fn new(at: AtTime) -> Self {
        Self {
            status_change_on: at.into(),
            status: SessionStatus::InvalidPasswordChange,
        }
    }
}

#[ta_objects::Dao]
#[dao(sql_table = "sessions")]
pub struct SessionNewTokenDB {
    pub status_change_on: StatusChange<AtTime>,
    pub status: SessionStatus,
}

impl SessionNewTokenDB {
    pub fn new(at: AtTime) -> Self {
        Self {
            status_change_on: at.into(),
            status: SessionStatus::InvalidNewToken,
        }
    }
}

// Auth requests operate on global identity tables (users/sessions), not
// project-scoped data; their services declare `scope = Global`.
