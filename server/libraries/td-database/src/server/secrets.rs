//
// Copyright 2026 Tabs Data Inc.
//

//! Password files persisted next to the data dir — its parent, because
//! `initdb` refuses a non-empty data dir. Explicit config credentials always
//! win over these.

use crate::error::DbError;
use std::path::Path;

const PASSWORD_FILE: &str = ".td_embedded_password";

const OWNER_PASSWORD_FILE_PREFIX: &str = ".td_embedded_owner_password.";

/// The persisted superuser password, generated and written on first boot.
pub(super) fn load_or_create_password(data_dir: &Path) -> Result<String, DbError> {
    load_or_create_secret(data_dir, PASSWORD_FILE)
}

/// The persisted superuser password; missing means never booted — an error.
pub fn load_password(data_dir: &Path) -> Result<String, DbError> {
    let path = secret_parent(data_dir)?.join(PASSWORD_FILE);
    std::fs::read_to_string(&path)
        .map(|s| s.trim().to_string())
        .map_err(|e| DbError::EmbeddedPostgres(format!("read password file: {e}")))
}

fn owner_password_file(schema: &str) -> String {
    format!("{OWNER_PASSWORD_FILE_PREFIX}{schema}")
}

/// `schema`'s persisted owner password, generated when absent — provisioning.
pub fn load_or_create_owner_password(data_dir: &Path, schema: &str) -> Result<String, DbError> {
    load_or_create_secret(data_dir, &owner_password_file(schema))
}

/// `schema`'s persisted owner password; missing means never provisioned — an
/// error.
pub fn load_owner_password(data_dir: &Path, schema: &str) -> Result<String, DbError> {
    let parent = secret_parent(data_dir)?;
    let path = parent.join(owner_password_file(schema));
    std::fs::read_to_string(&path)
        .map(|s| s.trim().to_string())
        .map_err(|e| DbError::EmbeddedPostgres(format!("read owner password file: {e}")))
}

fn secret_parent(data_dir: &Path) -> Result<&Path, DbError> {
    data_dir
        .parent()
        .ok_or_else(|| DbError::EmbeddedPostgres(format!("data_dir has no parent: {data_dir:?}")))
}

fn load_or_create_secret(data_dir: &Path, file: &str) -> Result<String, DbError> {
    let parent = secret_parent(data_dir)?;
    let path = parent.join(file);
    if path.exists() {
        return std::fs::read_to_string(&path)
            .map(|s| s.trim().to_string())
            .map_err(|e| DbError::EmbeddedPostgres(format!("read password file: {e}")));
    }
    std::fs::create_dir_all(parent)
        .map_err(|e| DbError::EmbeddedPostgres(format!("create password dir: {e}")))?;
    let password = td_common::random::password();
    std::fs::write(&path, &password)
        .map_err(|e| DbError::EmbeddedPostgres(format!("write password file: {e}")))?;
    Ok(password)
}
