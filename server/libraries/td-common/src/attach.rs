//
// Copyright 2025 Tabs Data Inc.
//

use crate::env::{TABSDATA_DEV_HOME_DIR, check_flag_env};
use crate::files::ROOT;
use config::{Config, File, FileFormat};
use serde::Deserialize;
use std::path::PathBuf;
use std::process::id;
use std::thread::sleep;
use std::time::{Duration, Instant};

pub use td_attach::attach;

// This function is called by the macro generated code. Therefor, identifying it as dead code is fine.
#[allow(dead_code)]
pub fn wait_for_attach(signal: &str) {
    const MAX_WAIT: u64 = 300;
    const SLEEP_TIME: u64 = 5;

    if !check_attach_env() && !check_attach_config(signal) {
        return;
    }
    println!("Entering into Wait for Attach function... '{signal}'");
    println!("Waiting for debugger to attach...: '{}'", id());
    let mut condition = false;
    let max_wait = Duration::from_secs(MAX_WAIT);
    let sleep_time = Duration::from_secs(SLEEP_TIME);
    let start_time = Instant::now();
    while !condition && start_time.elapsed() < max_wait {
        sleep(sleep_time);
        condition = false;
    }
    println!("Exiting from Wait for Attach function...");
}

/// Checks if Tabs Data No Wait flag is activated as an environment variable.
// When debugging, this environment variable can make the supervisor not to wait for workers.
pub fn check_nowait_env() -> bool {
    const TABSDATA_NOWAIT: &str = "TABSDATA_NOWAIT";
    check_flag_env(TABSDATA_NOWAIT)
}

/// Checks if Tabs Data Attach flag is activated as an environment variable.
// This function is called by the macro generated code. Therefore, identifying it as dead code is fine.
#[allow(dead_code)]
fn check_attach_env() -> bool {
    const TABSDATA_ATTACH: &str = "TABSDATA_ATTACH";
    check_flag_env(TABSDATA_ATTACH)
}

/// Checks if Tabs Data Attach flag is activated as a signal file.
// This function is called by the macro generated code. Therefore, identifying it as dead code is fine.
fn check_attach_config(filename: &str) -> bool {
    #[derive(Deserialize)]
    struct AttachConfig {
        attach: Option<String>,
    }

    const ATTACH_FOLDER: &str = "attach";

    const TRUE: &str = "true";
    const YES: &str = "yes";
    const ONE: &str = "1";

    let mut path = dirs::home_dir().unwrap_or_else(|| PathBuf::from(ROOT));
    path.push(TABSDATA_DEV_HOME_DIR);
    path.push(ATTACH_FOLDER);
    path.push(filename);
    if !path.exists() {
        return false;
    }
    let config = Config::builder()
        .add_source(File::with_name(path.to_str().unwrap()).format(FileFormat::Yaml))
        .build();
    let config = match config {
        Ok(c) => c,
        Err(_) => return false,
    };
    let config: AttachConfig = config
        .try_deserialize()
        .unwrap_or(AttachConfig { attach: None });

    matches!(config.attach,
        Some(ref val)
        if val.eq_ignore_ascii_case(TRUE)
        || val.eq_ignore_ascii_case(YES)
        || val == ONE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lazy_static::lazy_static;
    use std::env::{remove_var, set_var};
    use std::sync::{Mutex, MutexGuard};

    lazy_static! {
        static ref TEST_MUTEX: Mutex<()> = Mutex::new(());
    }

    struct EnvironmentGuard;

    impl Drop for EnvironmentGuard {
        fn drop(&mut self) {
            teardown_environment();
        }
    }

    fn setup_environment(value: &str) -> (MutexGuard<'static, ()>, EnvironmentGuard) {
        let guard = TEST_MUTEX.lock().unwrap();
        // Setting env vars is not thread-safe; use with care.
        unsafe {
            set_var("TABSDATA_ATTACH", value);
        }
        (guard, EnvironmentGuard)
    }

    fn teardown_environment() {
        // Setting env vars is not thread-safe; use with care.
        unsafe {
            remove_var("TABSDATA_ATTACH");
        }
    }

    #[test]
    fn test_check_attach_flag_true() {
        let (_guard, _env_guard) = setup_environment("true");
        assert!(check_attach_env());
    }

    #[test]
    fn test_check_attach_flag_yes() {
        let (_guard, _env_guard) = setup_environment("yes");
        assert!(check_attach_env());
    }

    #[test]
    fn test_check_attach_flag_1() {
        let (_guard, _env_guard) = setup_environment("1");
        assert!(check_attach_env());
    }

    #[test]
    fn test_check_attach_flag_false() {
        let (_guard, _env_guard) = setup_environment("false");
        assert!(!check_attach_env());
    }

    #[test]
    fn test_check_attach_flag_no() {
        let (_guard, _env_guard) = setup_environment("no");
        assert!(!check_attach_env());
    }

    #[test]
    fn test_check_attach_flag_0() {
        let (_guard, _env_guard) = setup_environment("0");
        assert!(!check_attach_env());
    }

    #[test]
    fn test_check_attach_flag_empty() {
        let (_guard, _env_guard) = setup_environment("");
        assert!(!check_attach_env());
    }

    #[test]
    fn test_check_attach_flag_not_set() {
        let (_guard, _env_guard) = setup_environment("");
        // Setting env vars is not thread-safe; use with care.
        unsafe {
            remove_var("ATTACH");
        }
        assert!(!check_attach_env());
    }
}
