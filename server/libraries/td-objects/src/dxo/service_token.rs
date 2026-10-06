//
// Copyright 2026 Tabsdata Inc.
//

//! `(user_id, slot)` is unique, so a rotation always has a free slot. The
//! row keeps only the [`TokenDigest`]; the plaintext appears once, in
//! [`TokenCreated`].

use crate::types::basic::{
    AtTime, Created, ExpiresInDays, LastUsed, ServiceToken, ServiceTokenId, TokenComment,
    TokenDigest, TokenExpiresOn, TokenHint, TokenScope, TokenSlot, TokenSystem, UserEnabled,
    UserId, UserIdName, UserName,
};
use ta_objects::FromDao;
use td_error::TdError;

#[ta_objects::Dao]
#[dao(
    table = "service_tokens",
    ops(select, insert, delete),
    unique(TokenDigest),
    unique(UserId, TokenSlot)
)]
pub struct ServiceTokenDB {
    #[dao(pk)]
    pub id: ServiceTokenId,
    pub user_id: UserId,
    pub slot: TokenSlot,
    pub digest: TokenDigest,
    pub hint: TokenHint,
    pub comment: Option<TokenComment>,
    pub created_on: Created<AtTime>,
    pub expires_on: TokenExpiresOn,
    pub last_used_on: Option<LastUsed<AtTime>>,
    pub scope: TokenScope,
    pub system: TokenSystem,
}

/// Carries the owner's enabled flag for verification.
#[ta_objects::Dao]
#[dao(
    view = "service_tokens__with_names",
    unique(TokenDigest),
    unique(UserId, TokenSlot)
)]
pub struct ServiceTokenDBWithNames {
    #[dao(flatten)]
    pub base: ServiceTokenDB,
    pub user_name: UserName,
    pub user_enabled: UserEnabled,
}

#[ta_objects::Dao]
#[dao(table = "service_tokens", ops(update(ServiceTokenDB)))]
pub struct ServiceTokenTouchDB {
    pub last_used_on: Option<LastUsed<AtTime>>,
}

#[ta_objects::Dto]
pub struct TokenCreate {
    pub expires_in_days: ExpiresInDays,
    pub comment: Option<TokenComment>,
}

/// The one payload carrying the token itself.
#[ta_objects::Dto]
pub struct TokenCreated {
    pub id: ServiceTokenId,
    pub token: ServiceToken,
    pub slot: TokenSlot,
    pub hint: TokenHint,
    pub expires_on: TokenExpiresOn,
    pub comment: Option<TokenComment>,
}

#[ta_objects::Dto]
#[dto(list(on = ServiceTokenDBWithNames))]
pub struct TokenRead {
    #[dto(list(pagination_by = "+"))]
    pub id: ServiceTokenId,
    #[dto(list(order_by))]
    pub slot: TokenSlot,
    pub hint: TokenHint,
    pub comment: Option<TokenComment>,
    pub created_on: Created<AtTime>,
    pub expires_on: TokenExpiresOn,
    pub last_used_on: Option<LastUsed<AtTime>>,
    pub scope: TokenScope,
    pub system: TokenSystem,
}

impl FromDao for TokenRead {
    type Dao = ServiceTokenDBWithNames;

    fn from_dao(dao: ServiceTokenDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.base.id,
            slot: dao.base.slot,
            hint: dao.base.hint,
            comment: dao.base.comment,
            created_on: dao.base.created_on,
            expires_on: dao.base.expires_on,
            last_used_on: dao.base.last_used_on,
            scope: dao.base.scope,
            system: dao.base.system,
        })
    }
}

/// About the presenting token only.
#[ta_objects::Dto]
pub struct TokenSelf {
    pub id: ServiceTokenId,
    pub slot: TokenSlot,
    pub user_name: UserName,
    pub hint: TokenHint,
    pub comment: Option<TokenComment>,
    pub created_on: Created<AtTime>,
    pub expires_on: TokenExpiresOn,
}

impl FromDao for TokenSelf {
    type Dao = ServiceTokenDBWithNames;

    fn from_dao(dao: ServiceTokenDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.base.id,
            slot: dao.base.slot,
            user_name: dao.user_name,
            hint: dao.base.hint,
            comment: dao.base.comment,
            created_on: dao.base.created_on,
            expires_on: dao.base.expires_on,
        })
    }
}

#[ta_objects::UrlParam]
pub struct TokenSlotParam {
    pub slot: TokenSlot,
}

#[ta_objects::UrlParam]
pub struct UserSlotParam {
    pub user: UserIdName,
    pub slot: TokenSlot,
}
