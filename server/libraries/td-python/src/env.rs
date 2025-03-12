//
// Copyright 2025 Tabs Data Inc.
//

use crate::error::PythonError::WrongEnvPath;
use rand::distr::Alphanumeric;
use rand::{rng, Rng};
use std::env;
use std::ffi::OsString;
use std::path::MAIN_SEPARATOR;
use std::string::ToString;
use td_error::TdError;

#[cfg(windows)]
pub const PATH_SEPARATOR: char = ';';

#[cfg(unix)]
pub const PATH_SEPARATOR: char = ':';

pub const ENV_PATH: &str = "PATH";

#[cfg(not(test))]
pub fn env_path() -> String {
    ENV_PATH.to_string()
}

#[cfg(test)]
pub fn env_path() -> String {
    let env_path_name = env();
    if let Ok(env_path) = env::var(ENV_PATH) {
        env::set_var(&env_path_name, env_path);
    }
    env_path_name
}

fn path_vec(env: Option<String>) -> (String, Vec<String>) {
    let env_path_name = env.unwrap_or_else(|| ENV_PATH.to_string());
    let env_path = env::var(&env_path_name).unwrap_or_else(|_| String::new());
    let env_paths: Vec<String> = env::split_paths(&env_path)
        .map(|p| {
            p.to_string_lossy()
                .trim_end_matches(MAIN_SEPARATOR)
                .to_string()
        })
        .collect();
    (env_path_name, env_paths)
}

pub fn env() -> String {
    rng()
        .sample_iter(&Alphanumeric)
        .take(16)
        .map(char::from)
        .collect()
}

pub fn join(paths: Vec<String>, env: String) -> Result<OsString, TdError> {
    match env::join_paths(paths) {
        Ok(new_env_path) => {
            env::set_var(env, &new_env_path);
            Ok(new_env_path)
        }
        Err(err) => Err(WrongEnvPath(err))?,
    }
}

pub fn prepend_in_path(path: &str, env: Option<String>) -> Result<OsString, TdError> {
    if path.trim().is_empty() {
        return Ok(env::var_os(env_path()).unwrap_or_default());
    }
    let (env_path_name, mut env_paths) = path_vec(env);
    let normalized_path = path.trim_end_matches(MAIN_SEPARATOR).to_string();
    env_paths.retain(|p| p != &normalized_path);
    env_paths.insert(0, normalized_path);
    join(env_paths, env_path_name)
}

pub fn remove_from_path(path: &str, env: Option<String>) -> Result<OsString, TdError> {
    if path.trim().is_empty() {
        return Ok(env::var_os(env_path()).unwrap_or_default());
    }
    let (env_path_name, mut env_paths) = path_vec(env);
    let normalized_path = path.trim_end_matches(MAIN_SEPARATOR);
    env_paths.retain(|p| p != normalized_path);
    match env::join_paths(env_paths) {
        Ok(new_env_path) => {
            env::set_var(&env_path_name, &new_env_path);
            Ok(new_env_path)
        }
        Err(err) => Err(WrongEnvPath(err))?,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::ffi::OsString;

    #[test]
    fn test_prepend_in_path_with_existing_path() {
        let env_path_name = env_path();
        let path = "/my/path";
        let old_path = env::var(&env_path_name).unwrap_or_default();
        env::set_var(
            &env_path_name,
            format!("{}{}{}", path, PATH_SEPARATOR, old_path),
        );
        let result = prepend_in_path(path, Some(env_path_name.to_string())).unwrap();
        let new_path = env::var(&env_path_name).unwrap();
        assert!(new_path.starts_with(path));
        assert_eq!(result, OsString::from(new_path));
    }

    #[test]
    fn test_prepend_in_path_with_new_path() {
        let env_path_name = env_path();
        let path = "/my/path";
        let result = prepend_in_path(path, Some(env_path_name.to_string())).unwrap();
        let new_path = env::var(&env_path_name).unwrap();
        assert!(new_path.starts_with(path));
        assert_eq!(result, OsString::from(new_path));
    }

    #[test]
    fn test_prepend_in_path_with_empty_path() {
        let env_path_name = env_path();
        let path = "";
        let old_path = env::var(&env_path_name).unwrap_or_default();
        let result = prepend_in_path(path, Some(env_path_name.to_string())).unwrap();
        assert_eq!(result, OsString::from(old_path));
    }

    #[test]
    fn test_prepend_in_path_with_invalid_characters() {
        let reserved_chars = r#"\ / : * ? " < > | "#;
        let control_chars: String = (0x00..=0x1F).map(|c| c as u8 as char).collect();
        let invalid_chars = format!("{}{}", reserved_chars, control_chars);

        let env_path_name = env_path();
        let path = invalid_chars;
        let result = prepend_in_path(&path, Some(env_path_name.to_string()));
        assert!(result.is_err());
    }
}
