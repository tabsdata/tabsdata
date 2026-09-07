//
// Copyright 2026 Tabsdata Inc.
//

//! Authorization extension point — Open Source build.
//!
//! The OS build contributes no extra authorization surface: [`register_ext`] is
//! a no-op and [`enable_extensions`] simply brings the built-in authz subsystem
//! online (the `sys_admin` reconciliation and the simple-mode representability
//! gate). The EE build ships a `te-authz` with the same public API that
//! registers additional authorization surface, so the shared `td-apiserver`
//! compiles against either.

use std::sync::Arc;
use td_authz::rbac::AuthzRegistry;
use td_authz::security::sys_admin::ensure_sys_admin_permissions;
use td_database::access::Global;
use td_database::domain::{Apiserver, DomainConnection};
use td_database::pool::TdPool;
use td_error::{TdError, td_error};
use td_security::config::PasswordHashingConfig;
use tracing::{error, info};

/// No-op in the OS build: the OS contributes no extension authz surface. The OS
/// owns the base [`AuthzRegistry::default`]; this returns it unchanged.
pub fn register_ext(registry: AuthzRegistry) -> AuthzRegistry {
    registry
}

#[td_error]
pub enum AuthzBootstrapError {
    #[error("Could not start the authz bootstrap transaction: {0}.")]
    TransactionBegin(String) = 5000,
    #[error("Could not commit the authz bootstrap transaction: {0}.")]
    TransactionCommit(String) = 5001,
    #[error(
        "Cannot start in 'simple' security mode: the current RBAC configuration is not \
         representable on the Simple surface. Revert the offending configuration or start in \
         'rbac' security mode. Offending rows ({count}):\n{reasons}"
    )]
    NotRepresentable { count: usize, reasons: String } = 5002,
}

/// Build the [`AuthzRegistry`] and bring it online: reconcile `sys_admin` and —
/// in `simple` security mode — verify the RBAC configuration is representable on
/// the Simple surface. Returns the shared registry for the API server context.
///
/// `pw_cfg` is unused in the OS build (no extension identities to seed); it is
/// accepted so the signature matches the EE crate and the shared `td-apiserver`
/// compiles against either.
pub async fn enable_extensions(
    db: &TdPool,
    simple_mode: bool,
    _pw_cfg: &PasswordHashingConfig,
) -> Result<Arc<AuthzRegistry>, TdError> {
    let registry = register_ext(AuthzRegistry::default());

    // Boot writes run as one committed transaction on the regular pool.
    let tx = db
        .begin()
        .await
        .map_err(|e| AuthzBootstrapError::TransactionBegin(e.to_string()))?;
    let mut conn = DomainConnection::<Apiserver, Global>::new(tx);
    ensure_sys_admin_permissions(&mut conn, registry.sys_admin_permissions()).await?;
    conn.commit()
        .await
        .map_err(|e| AuthzBootstrapError::TransactionCommit(e.to_string()))?;

    if simple_mode {
        ensure_representable(db, &registry).await?;
    } else {
        info!("Security mode 'rbac': RBAC management endpoints enabled");
    }

    Ok(Arc::new(registry))
}

/// Simple Security Mode requires the current RBAC configuration to be
/// representable on the flat Simple surface; refuse to start otherwise and list
/// the offending rows so the operator can revert or boot in `rbac` mode.
async fn ensure_representable(db: &TdPool, registry: &AuthzRegistry) -> Result<(), TdError> {
    let conn = db
        .begin_ro()
        .await
        .map_err(|e| AuthzBootstrapError::TransactionBegin(e.to_string()))?;
    let mut conn = DomainConnection::<Apiserver, Global>::new(conn);
    let reasons = td_authz::security::representability::check_representability(
        &mut conn,
        registry.sys_admin_permissions(),
    )
    .await?;
    if reasons.is_empty() {
        info!("Security mode 'simple': RBAC configuration is representable");
        Ok(())
    } else {
        let detail = reasons
            .iter()
            .map(|reason| format!("  - {reason}"))
            .collect::<Vec<_>>()
            .join("\n");
        error!(
            "Cannot start in 'simple' security mode; RBAC configuration is not representable:\n{detail}"
        );
        Err(AuthzBootstrapError::NotRepresentable {
            count: reasons.len(),
            reasons: detail,
        })?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_ext_is_a_no_op() {
        let registry = AuthzRegistry::default();
        let before = registry.entity_types().collect::<Vec<_>>();
        let registry = register_ext(registry);
        let after = registry.entity_types().collect::<Vec<_>>();
        assert_eq!(before, after);
    }
}
