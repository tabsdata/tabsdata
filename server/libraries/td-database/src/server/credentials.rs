//
// Copyright 2026 Tabs Data Inc.
//

//! [`EMBEDDED_CREDENTIAL`] resolution against the persisted password files.

use crate::config::EMBEDDED_CREDENTIAL;
use crate::error::DbError;

/// Which persisted password [`EMBEDDED_CREDENTIAL`] names.
pub(crate) enum EmbeddedPassword<'a> {
    /// The superuser password baked in at first boot.
    Superuser,
    /// A schema owner's password (load-only, the serving path).
    Owner { schema: &'a str },
    /// A schema owner's password, generated when absent (the provisioning path).
    OwnerCreate { schema: &'a str },
}

/// Explicit values pass through, [`EMBEDDED_CREDENTIAL`] loads the named
/// persisted password (a hard error when it cannot resolve), absence stays
/// `None`.
pub(crate) fn resolve_password(
    value: Option<&str>,
    source: EmbeddedPassword<'_>,
) -> Result<Option<String>, DbError> {
    match value {
        Some(EMBEDDED_CREDENTIAL) => {
            #[cfg(feature = "embedded-postgres")]
            {
                use super::{
                    database_dir, load_or_create_owner_password, load_owner_password, load_password,
                };
                let dir = database_dir()?;
                let password = match source {
                    EmbeddedPassword::Superuser => load_password(&dir)?,
                    EmbeddedPassword::Owner { schema } => load_owner_password(&dir, schema)?,
                    EmbeddedPassword::OwnerCreate { schema } => {
                        load_or_create_owner_password(&dir, schema)?
                    }
                };
                Ok(Some(password))
            }
            #[cfg(not(feature = "embedded-postgres"))]
            {
                let _ = source;
                Err(DbError::MissingCredentials)
            }
        }
        Some(password) => Ok(Some(password.to_owned())),
        None => Ok(None),
    }
}
