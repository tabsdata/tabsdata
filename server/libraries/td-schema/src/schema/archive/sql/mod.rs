//
// Copyright 2026 Tabsdata Inc.
//

//! SQL machinery only archived schema versions use.

pub(crate) mod rls;
pub(crate) mod rls_v4;
pub(crate) mod rollup;
pub(crate) mod rollup_v13;
pub(crate) mod seed_current;
pub(crate) mod trigger;

pub(crate) use rollup::{Logical, Parent, Physical, RollupArm, RollupDef};
pub(crate) use seed_current::SeedCurrent;
pub(crate) use trigger::{ImmutableTriggerDef, MaintainCurrentTriggerDef, StickyTerminalsTriggerDef, VersionedTriggerDef};
