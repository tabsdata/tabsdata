//
// Copyright 2025 Tabsdata Inc.
//

//! API server edition extension. Routes self-register into td-apiserver's
//! inventory registry; the open-source edition adds none. The enterprise
//! edition swaps this crate to register its own.

pub mod config;

/// Anchor symbol: td-apiserver holds a `#[used]` reference to it so the
/// linker keeps this crate's route registrations in the final link.
pub fn link() {}

/// URL prefixes of the paths this edition contributes beyond td-apiserver's
/// own — what its doc completeness and service-token route tests leave to
/// this edition.
/// The open-source edition contributes none.
pub const EDITION_PATH_PREFIXES: &[&str] = &[];
