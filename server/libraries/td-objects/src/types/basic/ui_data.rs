//
// Copyright 2026 Tabsdata Inc.
//

use ta_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};
use uuid::Uuid;

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub struct UiDataEtag(Uuid);

impl UiDataEtag {
    /// A version never issued before, so a stale etag cannot match a newer
    /// write whatever the clock does.
    pub fn mint() -> Self {
        Self(Uuid::now_v7())
    }
}

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub struct UiDataDocument(String);
