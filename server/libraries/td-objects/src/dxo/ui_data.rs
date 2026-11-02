//
// Copyright 2026 Tabsdata Inc.
//

//! One opaque UI document per user, written under optimistic concurrency:
//! every write mints a new [`UiDataEtag`], and a conditional write
//! replaces the document only while the stored etag is the one it was given.

use crate::types::basic::{AtTime, Modified, UiDataDocument, UiDataEtag, UserId};

#[ta_objects::Dao]
#[dao(table = "user_ui_data", ops(select, insert, upsert(UserId), delete))]
pub struct UserUiDataDB {
    #[dao(pk)]
    pub user_id: UserId,
    pub etag: UiDataEtag,
    pub modified_on: Modified<AtTime>,
    pub data: UiDataDocument,
}

#[ta_objects::Dao]
#[dao(table = "user_ui_data", ops(update(UserUiDataDB)))]
pub struct UserUiDataUpdateDB {
    pub etag: UiDataEtag,
    pub modified_on: Modified<AtTime>,
    pub data: UiDataDocument,
}

impl From<&UserUiDataDB> for UserUiDataUpdateDB {
    fn from(row: &UserUiDataDB) -> Self {
        Self {
            etag: row.etag,
            modified_on: row.modified_on,
            data: row.data.clone(),
        }
    }
}

/// What a write's `If-Match` asks for.
#[derive(Debug, Clone)]
pub enum UiDataPrecondition {
    Absent,
    Etag(UiDataEtag),
    /// Present but not an etag this server could have issued.
    NeverMatches,
}

#[derive(Debug, Clone)]
pub struct UiDataPut {
    pub data: UiDataDocument,
    pub precondition: UiDataPrecondition,
    /// Write unconditionally, ignoring `precondition`.
    pub force: bool,
}

pub enum UiDataWrite {
    Created(UserUiDataDB),
    Replaced(UserUiDataDB),
    /// The stored document, if any, so the client can merge and retry.
    PreconditionFailed(Option<UserUiDataDB>),
    /// A document exists and the write named no etag: the current one.
    PreconditionRequired(UiDataEtag),
}
