//
// Copyright 2026 Tabsdata Inc.
//

//! Internal SQL building blocks shared by the trait impls in
//! [`crate::generic`] and [`crate::daos`]. Nothing here is part of the
//! crate's public API; everything is `pub(crate)` so callers can't
//! reach in and bypass the safe trait surface.

pub(crate) mod chunked;
pub(crate) mod recursive;
pub(crate) mod versioned;
