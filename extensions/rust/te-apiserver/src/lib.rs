//
// Copyright 2025 Tabsdata Inc.
//

//! API server edition extension. The open-source edition adds no routes; the
//! enterprise edition swaps this crate for one whose routes its `te-edition`
//! hands td_apiserver.

pub mod config;

/// URL prefixes of the paths this edition contributes beyond td-apiserver's
/// own — what its doc completeness and service-token route tests leave to
/// this edition.
/// The open-source edition contributes none.
pub const EDITION_PATH_PREFIXES: &[&str] = &[];
