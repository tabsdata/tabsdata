//
// Copyright 2026 Tabsdata Inc.
//

//! `function_patches__with_names` resolves a patch's author, so the patch list
//! surfaces a user name like every other listing.
//!
//! The view is a derived object: the ladder drops every derived object before
//! the rungs and recreates them from `current/` afterwards, so creating it
//! here would collide with that rebuild. The rung carries the version bump
//! alone -- what an existing database needs to reach the rebuild.

use crate::schema::render::update_db_version;
use crate::schema::version::SchemaDelta;

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 6;
    const TO_VERSION: usize = 7;

    fn retired() -> Vec<String> {
        vec![]
    }

    fn up() -> Vec<String> {
        vec![update_db_version(Self::TO_VERSION)]
    }
}
