//
// Copyright 2026 Tabs Data Inc.
//

//! Security-mode support: the RBAC->Simple representability check.
//!
//! Switching Simple -> RBAC is always safe. Switching RBAC -> Simple is only
//! possible if the current role/permission configuration is *representable* on
//! the Simple surface. [`representability`] reads `roles` + `role_permission`
//! and returns the list of human-readable reasons the configuration is not
//! representable (empty = representable). The apiserver runs this check at
//! startup when `security_mode = simple`.

pub mod representability;
pub mod sys_admin;
