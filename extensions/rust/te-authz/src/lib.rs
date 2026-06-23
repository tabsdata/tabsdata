//
// Copyright 2026 Tabs Data Inc.
//

//! Authorization extension: an entity kind and permission defined entirely
//! outside `td-authz` and contributed to its [`AuthzRegistry`].
//!
//! # Wiring
//!
//! The only touch outside this crate is in the main path: the OS builds the
//! base (built-in) registry and hands it to [`register_ext`], which returns it
//! augmented with this extension's kinds — so the OS stays in control of the
//! default and each extension contributes its own surface.
//!
//! ```ignore
//! use std::sync::Arc;
//! use td_authz::AuthzContext;
//! use td_authz::rbac::AuthzRegistry;
//!
//! let registry = te_authz::register_ext(AuthzRegistry::default());
//! let auth_context = AuthzContext::with_registry(Arc::new(registry));
//! ```
//!

use td_authz::rbac::AuthzRegistry;

/// Augment `registry`
pub fn register_ext(registry: AuthzRegistry) -> AuthzRegistry {
    registry
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_register_ext() {
        let registry = AuthzRegistry::default();
        let before = registry.entity_types().collect::<Vec<_>>();
        let registry = register_ext(registry);
        let after = registry.entity_types().collect::<Vec<_>>();
        assert_eq!(before, after);
    }
}
