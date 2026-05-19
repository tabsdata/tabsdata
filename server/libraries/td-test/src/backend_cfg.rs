//
// Copyright 2026 Tabs Data Inc.
//

//! Cfg-toggle macros for backend gating. The proc macro `#[td_test::test(db)]`
//! wraps each per-backend test arm in `::td_test::__cfg_<backend>! { ... }`
//! instead of emitting `#[cfg(<backend>)]` directly at the call site. The
//! `cfg(sqlite|postgres|mysql)` lookup then happens *here* (where the cfg
//! aliases are declared via td-test's `build.rs` trampoline that propagates
//! td-database's enabled backend features) instead of in every consuming
//! crate — downstream callers don't need their own build.rs.
//!
//! Each macro has two definitions on mutually exclusive cfgs; exactly one
//! compiles into td-test. Consumers always invoke `::td_test::__cfg_<backend>!`
//! and get the right behavior based on whichever was compiled in.
//!
//! The macros are hoisted to td-test's crate root by `#[macro_export]`, so
//! this module has no public items — it exists purely to keep the macro
//! definitions out of `lib.rs`.

#[doc(hidden)]
#[cfg(sqlite)]
#[macro_export]
macro_rules! __cfg_sqlite {
    ($($body:tt)*) => { $($body)* };
}

#[doc(hidden)]
#[cfg(not(sqlite))]
#[macro_export]
macro_rules! __cfg_sqlite {
    ($($body:tt)*) => {};
}

#[doc(hidden)]
#[cfg(postgres)]
#[macro_export]
macro_rules! __cfg_postgres {
    ($($body:tt)*) => { $($body)* };
}

#[doc(hidden)]
#[cfg(not(postgres))]
#[macro_export]
macro_rules! __cfg_postgres {
    ($($body:tt)*) => {};
}

#[doc(hidden)]
#[cfg(mysql)]
#[macro_export]
macro_rules! __cfg_mysql {
    ($($body:tt)*) => { $($body)* };
}

#[doc(hidden)]
#[cfg(not(mysql))]
#[macro_export]
macro_rules! __cfg_mysql {
    ($($body:tt)*) => {};
}
