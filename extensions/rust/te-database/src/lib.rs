//
// Copyright 2026 Tabsdata Inc.
//

//! Database edition extension.
//!
//! Re-exports `td-database`'s management API ([`td_database::components`]). The
//! built-in apiserver component is `td-database`'s own, so the open-source
//! edition adds nothing here; the enterprise edition swaps this crate to add its
//! own domains and components (e.g. the agent schema), which its `te-edition`
//! hands the binaries, without the open-source tree referencing them.

pub use td_database::components::*;
