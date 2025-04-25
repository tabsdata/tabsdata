//
// Copyright 2025 Tabs Data Inc.
//

use crate::env::{get_home_dir, TABSDATA_HOME_DIR};
use crate::server::INSTANCE_PATH_ENV;
use config::{Config, File};
use once_cell::sync::Lazy;
use serde::Deserialize;
use std::collections::HashMap;
use std::env::var_os;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::SystemTime;
use std::{env, fs, io};
use tracing::{debug, info, warn};

pub const TRUE: &str = "true";
pub const YES: &str = "yes";
pub const ONE: &str = "1";
pub const ON: &str = "on";

pub const FALSE: &str = "true";
pub const NOT: &str = "not";
pub const ZERO: &str = "0";
pub const OFF: &str = "off";

pub const SETTINGS_FILE: &str = "settings.yaml";

pub const ENV_LOG_MODE: &str = "env_log_mode";
pub const LOG_WITH_ANSI: &str = "log_with_ansi";

pub const DEFAULT_SETTINGS: &str =
    include_str!("../../../binaries/td-server/resources/settings/settings.yaml");

static SETTINGS: LazyLock<Settings> = LazyLock::new(|| {
    Config::builder()
        .add_source(File::from_str(DEFAULT_SETTINGS, config::FileFormat::Yaml))
        .build()
        .ok()
        .and_then(|config| config.try_deserialize().ok())
        .unwrap_or_default()
});

pub static MANAGER: Lazy<Arc<SettingsManager>> = Lazy::new(|| Arc::new(SettingsManager::new()));

#[derive(Debug, Deserialize, Clone, Default)]
pub struct Settings {
    #[serde(flatten)]
    settings: HashMap<String, String>,
}

#[derive(Debug)]
pub struct SettingsData {
    settings: Settings,
    modified: SystemTime,
}

pub struct SettingsManager {
    settings: RwLock<SettingsData>,
    instance: RwLock<Option<String>>,
}

impl SettingsManager {
    fn new() -> Self {
        dump();
        let instance = var_os(INSTANCE_PATH_ENV).map(|value| value.to_string_lossy().into_owned());
        let settings = match &instance {
            Some(path) => load(path).unwrap_or_default(),
            None => Settings::default(),
        };
        Self {
            settings: RwLock::new(SettingsData {
                settings,
                modified: SystemTime::now(),
            }),
            instance: RwLock::new(instance),
        }
    }

    fn init(&self) {
        let instance = self.instance.read().unwrap().clone().or_else(|| {
            var_os(INSTANCE_PATH_ENV).map(|value| value.to_string_lossy().into_owned())
        });

        if let Some(folder) = instance {
            let file = Path::new(&folder).join(SETTINGS_FILE);
            if file.exists() {
                if let Ok(modified) = fs::metadata(&file).and_then(|metadata| metadata.modified()) {
                    if self.settings.read().unwrap().modified < modified {
                        if let Some(settings) = load(&folder) {
                            let mut data_guard = self.settings.write().unwrap();
                            *data_guard = SettingsData { settings, modified };
                            debug!(
                                "Loaded new instance settings. Current settings:\n{}",
                                self.table()
                            );
                        } else {
                            warn!(
                                "Failed loading new instance settings. Keeping the current ones. Current settings:\n{}",
                                self.table()
                            );
                        }
                        let mut path_guard = self.instance.write().unwrap();
                        if path_guard.is_none() {
                            *path_guard = Some(folder);
                        }
                    }
                }
            } else {
                if !self.settings.read().unwrap().settings.settings.is_empty() {
                    let mut data_guard = self.settings.write().unwrap();
                    data_guard.settings.settings.clear();
                }
                if self.instance.read().unwrap().is_none() {
                    let mut path_guard = self.instance.write().unwrap();
                    *path_guard = Some(folder);
                }
            }
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        // First, we check for environment variable TD_<key>.
        // Search is made all uppercase.
        if let Ok(value) = env::var(format!("TD_{}", key.to_uppercase())) {
            return Some(value);
        }

        // Second, we check for environment variable TD__<instance>_<key>.
        // Search is made all uppercase.
        if let Some(value) =
            var_os(INSTANCE_PATH_ENV).map(|value| value.to_string_lossy().into_owned())
        {
            let path = PathBuf::from(&value);
            if let Some(instance) = path.file_name() {
                if let Ok(value) = env::var(format!(
                    "TD__{}_{}",
                    instance.to_string_lossy().to_uppercase(),
                    key.to_uppercase()
                )) {
                    return Some(value);
                }
            }
        }

        // Before proceeding, we check:
        // - If instance path is already available, loading instance settings.yaml;
        // - If instance settings.yaml has changed, reloading it in this case.
        // Search is made all lowercase.
        self.init();

        // Third, we check for key in instance settings.yaml.
        let guard = self.settings.read().unwrap();
        if let Some(value) = guard.settings.settings.get(&key.to_lowercase()) {
            return Some(value.clone());
        }

        // Fourth, we check for key in default embedded settings.yaml.
        // Search is made all lowercase.
        SETTINGS.settings.get(&key.to_lowercase()).cloned()
    }

    fn table(&self) -> String {
        let map = &self.settings.read().unwrap().settings.settings;
        if map.is_empty() {
            "No instance settings specified.".to_string()
        } else {
            map.iter()
                .map(|(key, value)| format!("- '{}': '{}'", key, value))
                .collect::<Vec<String>>()
                .join("\n")
        }
    }
}

fn load(folder: &str) -> Option<Settings> {
    let file = Path::new(folder).join(SETTINGS_FILE);
    Config::builder()
        .add_source(File::from(file).required(false))
        .build()
        .and_then(|config| config.try_deserialize::<Settings>())
        .ok()
}

fn dump() {
    if let Some(value) = var_os(INSTANCE_PATH_ENV).map(|value| value.to_string_lossy().into_owned())
    {
        let path = PathBuf::from(&value);
        if let Some(instance) = path.file_name() {
            let source = get_home_dir()
                .join(TABSDATA_HOME_DIR)
                .join(format!("settings_{}.yaml", instance.to_string_lossy()));
            if source.exists() {
                let target = path.join(SETTINGS_FILE);
                if target.exists() {
                    return;
                    /*
                    fs::remove_file(&target).unwrap_or_else(|e| {
                        panic!("Unable to remove default instance settings seed: {}", e)
                    });
                     */
                }
                if let Some(parent) = target.parent() {
                    fs::create_dir_all(parent).unwrap_or_else(|e| {
                        panic!("Unable to prepare instance settings seed: {}", e)
                    });
                }
                move_file(&source, &target)
                    .unwrap_or_else(|e| panic!("Unable to relocate instance settings seed: {}", e));
            }
        }
    }
}

fn move_file(source: &Path, target: &Path) -> io::Result<()> {
    if let Err(e) = fs::rename(source, target) {
        if e.kind() == io::ErrorKind::CrossesDevices {
            info!("Detected a cross-device link! Falling back to copy + remove.");
            fs::copy(source, target)?;
            fs::remove_file(source)?;
        } else {
            return Err(e);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::distr::Alphanumeric;
    use rand::{rng, Rng};
    use serde_yaml::to_string;
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;
    use std::thread::sleep;
    use std::time::Duration;
    use std::{env, panic};
    use tempfile::TempDir;

    pub const SLEEP_TIME: u64 = 50;

    fn yaml(key: Option<&str>, value: Option<&str>) -> PathBuf {
        let instance_folder = TempDir::new().expect("Folder creation error");
        let settings_path = instance_folder.path().join(SETTINGS_FILE);
        let mut settings_data = HashMap::new();
        if let (Some(k), Some(v)) = (key, value) {
            settings_data.insert(k.to_string(), v.to_string());
        }
        if !settings_data.is_empty() {
            let settings_content = to_string(&settings_data).expect("Serialization error");
            sleep(Duration::from_millis(SLEEP_TIME));
            let mut settings_file = File::create(&settings_path).expect("File creation error");
            settings_file
                .write_all(settings_content.as_bytes())
                .expect("File write error");
            settings_file.flush().expect("Failed to flush data to disk");
            sleep(Duration::from_millis(SLEEP_TIME));
        }

        instance_folder.into_path()
    }

    fn value() -> String {
        rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .collect()
    }

    #[test]
    fn test_get() {
        let instance = yaml(None, None);

        let result = panic::catch_unwind(|| {
            env::set_var(INSTANCE_PATH_ENV, instance.to_string_lossy().to_string());

            sleep(Duration::from_millis(SLEEP_TIME));

            let value_seed_ins = value();

            let seed_home = get_home_dir().join(TABSDATA_HOME_DIR);
            if !seed_home.exists() {
                fs::create_dir_all(&seed_home).expect("Hosting error");
            }
            let seed_path = get_home_dir().join(TABSDATA_HOME_DIR).join(format!(
                "settings_{}.yaml",
                instance.file_name().unwrap().to_string_lossy()
            ));
            let mut seed_data = HashMap::new();
            seed_data.insert(ENV_LOG_MODE, &value_seed_ins);
            let seed_content = to_string(&seed_data).expect("Serialization error");
            sleep(Duration::from_millis(SLEEP_TIME));
            let mut seed_file = File::create(&seed_path).expect("File creation error");
            seed_file
                .write_all(seed_content.as_bytes())
                .expect("File write error");
            seed_file.flush().expect("Failed to flush data to disk");
            sleep(Duration::from_millis(SLEEP_TIME));

            let setting = MANAGER.get(ENV_LOG_MODE);
            assert_eq!(
                setting,
                Some(value_seed_ins.to_string()),
                "(1) Expected {:?}, but got {:?}",
                Some(value_seed_ins.to_string()),
                setting
            );

            let value_env = value();
            let value_env_ins = value();
            let value_file_ins = value();
            let settings_path = instance.join(SETTINGS_FILE);
            let mut settings_data = HashMap::new();
            settings_data.insert(ENV_LOG_MODE, &value_file_ins);
            let settings_content = to_string(&settings_data).expect("Serialization error");
            sleep(Duration::from_millis(SLEEP_TIME));
            let mut settings_file = File::create(&settings_path).expect("File creation error");
            settings_file
                .write_all(settings_content.as_bytes())
                .expect("File write error");
            settings_file.flush().expect("Failed to flush data to disk");
            sleep(Duration::from_millis(SLEEP_TIME));

            env::set_var(format!("TD_{}", ENV_LOG_MODE.to_uppercase()), &value_env);
            env::set_var(
                format!(
                    "TD__{}_{}",
                    &instance
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .to_uppercase(),
                    &ENV_LOG_MODE.to_uppercase()
                ),
                &value_env_ins,
            );

            let setting = MANAGER.get(ENV_LOG_MODE);
            assert_eq!(
                setting,
                Some(value_env.to_string()),
                "(2) Expected {:?}, but got {:?}",
                Some(value_env.to_string()),
                setting
            );

            env::remove_var(format!("TD_{}", ENV_LOG_MODE.to_uppercase()));

            let setting = MANAGER.get(ENV_LOG_MODE);
            assert_eq!(
                setting,
                Some(value_env_ins.to_string()),
                "(3) Expected {:?}, but got {:?}",
                Some(value_env_ins.to_string()),
                setting
            );
            env::remove_var(format!(
                "TD__{}_{}",
                &instance
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .to_uppercase(),
                ENV_LOG_MODE.to_uppercase()
            ));

            let setting = MANAGER.get(ENV_LOG_MODE);
            assert_eq!(
                setting,
                Some(value_file_ins.to_string()),
                "(4) Expected {:?}, but got {:?}",
                Some(value_file_ins.to_string()),
                setting
            );

            sleep(Duration::from_millis(SLEEP_TIME));
            fs::rename(
                instance.join(SETTINGS_FILE),
                instance.join(format!("{}_", SETTINGS_FILE)),
            )
            .expect("Rename error");
            sleep(Duration::from_millis(SLEEP_TIME));

            let setting = MANAGER.get(ENV_LOG_MODE);
            assert_eq!(
                setting,
                Some("name".to_string()),
                "(5) Expected {:?}, but got {:?}",
                Some("name".to_string()),
                setting
            );

            let value_env = value();
            let value_env_ins = value();
            let value_file_ins = value();
            sleep(Duration::from_millis(SLEEP_TIME));
            fs::rename(
                instance.join(format!("{}_", SETTINGS_FILE)),
                instance.join(SETTINGS_FILE),
            )
            .expect("Rename error");
            sleep(Duration::from_millis(SLEEP_TIME));

            env::set_var(format!("TD_{}", ENV_LOG_MODE.to_uppercase()), &value_env);
            env::set_var(
                format!(
                    "TD__{}_{}",
                    &instance
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .to_uppercase(),
                    &ENV_LOG_MODE.to_uppercase()
                ),
                &value_env_ins,
            );
            let settings_path = instance.join(SETTINGS_FILE);
            let mut settings_data = HashMap::new();
            settings_data.insert(ENV_LOG_MODE, &value_file_ins);
            let settings_content = to_string(&settings_data).expect("Serialization error");
            sleep(Duration::from_millis(SLEEP_TIME));
            let mut settings_file = File::create(&settings_path).expect("File creation error");
            settings_file
                .write_all(settings_content.as_bytes())
                .expect("File write error");
            settings_file.flush().expect("Failed to flush data to disk");
            sleep(Duration::from_millis(SLEEP_TIME));

            let setting = MANAGER.get(ENV_LOG_MODE);
            assert_eq!(
                setting,
                Some(value_env.to_string()),
                "(6) Expected {:?}, but got {:?}",
                Some(value_env.to_string()),
                setting
            );

            env::remove_var(format!("TD_{}", ENV_LOG_MODE.to_uppercase()));

            let setting = MANAGER.get(ENV_LOG_MODE);
            assert_eq!(
                setting,
                Some(value_env_ins.to_string()),
                "(7) Expected {:?}, but got {:?}",
                Some(value_env_ins.to_string()),
                setting
            );

            env::remove_var(format!(
                "TD__{}_{}",
                &instance
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .to_uppercase(),
                ENV_LOG_MODE.to_uppercase()
            ));

            let setting = MANAGER.get(ENV_LOG_MODE);
            assert_eq!(
                setting,
                Some(value_file_ins.to_string()),
                "(8) Expected {:?}, but got {:?}",
                Some(value_file_ins.to_string()),
                setting
            );

            sleep(Duration::from_millis(SLEEP_TIME));
            fs::rename(
                instance.join(SETTINGS_FILE),
                instance.join(format!("{}_", SETTINGS_FILE)),
            )
            .expect("Rename error");
            sleep(Duration::from_millis(SLEEP_TIME));

            let setting = MANAGER.get(ENV_LOG_MODE);
            assert_eq!(
                setting,
                Some("name".to_string()),
                "(9) Expected {:?}, but got {:?}",
                Some("name".to_string()),
                setting
            );

            sleep(Duration::from_millis(SLEEP_TIME));
            fs::rename(
                instance.join(format!("{}_", SETTINGS_FILE)),
                instance.join(SETTINGS_FILE),
            )
            .expect("Rename error");
            sleep(Duration::from_millis(SLEEP_TIME));

            for i in 0..8 {
                let value_file_ins = value();

                let settings_path = instance.join(SETTINGS_FILE);
                let mut settings_data = HashMap::new();
                settings_data.insert(ENV_LOG_MODE, &value_file_ins);
                let settings_content = to_string(&settings_data).expect("Serialization error");
                sleep(Duration::from_millis(SLEEP_TIME));
                let mut settings_file = File::create(&settings_path).expect("File creation error");
                settings_file
                    .write_all(settings_content.as_bytes())
                    .expect("File write error");
                settings_file.flush().expect("Failed to flush data to disk");
                sleep(Duration::from_millis(SLEEP_TIME));

                let mut rng = rng();
                let time = rng.random_range(50..=100);
                sleep(Duration::from_millis(time));

                let setting = MANAGER.get(ENV_LOG_MODE);
                assert_eq!(
                    setting,
                    Some(value_file_ins.to_string()),
                    "(10 - {}) Expected {:?}, but got {:?}",
                    i,
                    Some(value_file_ins.to_string()),
                    setting
                );
            }

            env::remove_var(INSTANCE_PATH_ENV);
            env::remove_var(format!("TD_{}", ENV_LOG_MODE.to_uppercase()));
            env::remove_var(format!(
                "TD__{}_{}",
                &instance
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .to_uppercase(),
                ENV_LOG_MODE.to_uppercase()
            ));
        });

        if result.is_err() {
            println!("Test failed. Gathering context values...");

            println!("\nEnvironment variables:\n");
            for (key, value) in env::vars() {
                println!("   - '{}': '{}'", key, value);
            }

            let file = instance.join(SETTINGS_FILE);
            if file.exists() {
                match fs::read_to_string(file) {
                    Ok(contents) => println!("\nSettings file:\n\n{}", contents),
                    Err(e) => println!("Wrong contents in virtual instance settings: {}", e),
                }
            } else {
                println!("No tabsdata virtual instance settings");
            }

            panic::resume_unwind(result.err().unwrap());
        }
    }
}
