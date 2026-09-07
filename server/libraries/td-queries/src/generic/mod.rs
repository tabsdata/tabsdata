//
// Copyright 2026 Tabsdata Inc.
//

//! The grain surfaces. Each wrapper struct names the executor, so the bound
//! tying it to the DAO sits on the impl block once instead of on every
//! method; the unversioned default grain has no wrapper, its operations are
//! the [`Queries`](crate::Queries) methods themselves.
//!
//! The open-interval predicate is the `'infinity'` literal, never a bind
//! parameter: that is what matches the `WHERE valid_to = 'infinity'` partial
//! indexes.
//!
//! The listing bases carry no ordering of their own: pagination only appends,
//! so a DAO default order left in front would outrank the keyset column and
//! break paging.
//!
//! The list methods take no visibility filter: row visibility is the
//! connection's business — RLS scopes project rows, and cross-project reads
//! narrow by the requester's viewable projects before querying.

pub mod as_of;
pub mod chunked;
pub mod data_version;
pub mod for_update;
pub mod recursive;
mod row_locks;
pub mod scd2;
pub mod timeline;
