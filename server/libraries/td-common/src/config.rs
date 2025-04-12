//
// Copyright 2024 Tabs Data Inc.
//

use crate::cli::{move_to_dir, obtain_config_dir, obtain_current_dir};
use crate::env::{get_current_dir, get_home_dir, get_user_name, TABSDATA_HOME_DIR};
use config::{File, FileFormat};
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::path::PathBuf;
use tracing::{error, info};

/// Marker trait to define a configuration.
pub trait Config: Sized + Default + Serialize + for<'a> Deserialize<'a> {
    /// Returns the configuration as a YAML string.
    fn as_yaml(&self) -> String {
        let s = serde_yaml::to_string(self).expect("Could not create YAML for config");
        match s.starts_with("{}") {
            true => String::from("\n"),
            false => s,
        }
    }
}

/// Validates the configuration name. It must be an alphanumeric word,
/// and it can have hyphens '-' in it.
fn assert_config_name(config_name: &str) {
    // check config_name is a valid word.
    if !config_name.chars().all(|c| c.is_alphanumeric() || c == '-') {
        panic!(
            "Invalid config name, must be an alphanumeric word, it may have '-' dashes: {}",
            config_name
        );
    }
}

const CONFIG: &str = "config";

const EXTENSION: &str = "yaml";

/// Loads the worker configuration.
///
/// The configuration is created with the following order, where the last one has precedence:
/// - 1. Built-in default values ([`Default`] trait).
/// - 2. 'config.yaml' file in the current directory (optional).
/// - 3. 'config-<username>.yaml' file in the current directory (optional).
/// - 4. '<homedir>/.tabsdata/<config_name>.yaml' file (optional).
/// - 5. Environment variables with the prefix `<CONFIG_NAME>_` (optional).
///
/// The `config_name' must be an alphanumeric word, and it can have hyphens '-' in it.
///
/// For environment variables lookups, names are fully uppercased and hyphens '-' are
/// replaced with underscores '_'.
pub fn load_config<T: Config>(config_name: &str, config_folder: Option<PathBuf>, stdin: bool) -> T {
    let current_folder = obtain_current_dir();
    let _ = (match config_folder {
        None => move_to_dir(obtain_config_dir()),
        Some(_) => move_to_dir(config_folder),
    })
    .is_ok();

    assert_config_name(config_name);

    // built-in default config
    let default_config = &T::default();

    let current_dir = get_current_dir();

    // app config file in the current directory
    let app_config_file = current_dir.join(CONFIG).with_extension(EXTENSION);

    // user's name, replacing ' ' spaces with '-' underscores.
    let user_name = get_user_name().replace(' ', "-");

    // app user config file in the current directory
    let app_user_config_file = current_dir
        .join(format!("{}_{}", CONFIG, user_name))
        .with_extension(EXTENSION);

    // app config in user home's .tabsdata directory.
    let home_dir_config_file = get_home_dir()
        .join(TABSDATA_HOME_DIR)
        .join(config_name)
        .with_extension(EXTENSION);

    // environment variables prefix, replacing '-' hyphens from config name with '_' underscores.
    let app_env_prefix = config_name.replace('-', "_");

    let mut config_builder = config::Config::builder()
        .add_source(config::Config::try_from(&default_config).unwrap())
        .add_source(config::File::with_name(app_config_file.to_str().unwrap()).required(false))
        .add_source(config::File::with_name(app_user_config_file.to_str().unwrap()).required(false))
        .add_source(config::File::with_name(home_dir_config_file.to_str().unwrap()).required(false))
        .add_source(config::Environment::with_prefix(&app_env_prefix));

    // stdin if flagged to use it
    if stdin {
        info!("Reading from stdin as an additional source of configuration");
        let mut stdin_config = String::new();
        if let Err(err) = std::io::stdin().read_to_string(&mut stdin_config) {
            error!("Failed to read stdin config: {:?}", err);
            panic!("Exiting due to configuration ingestion failure.");
        }
        let stdin_config_stream = File::from_str(&stdin_config, FileFormat::Yaml);
        config_builder = config_builder.add_source(stdin_config_stream);
    }

    let config = match config_builder.build() {
        Ok(built_config) => match built_config.try_deserialize() {
            Ok(deserialized_config) => deserialized_config,
            Err(e) => {
                error!("Failed to deserialize config: {:?}", e);
                panic!("Exiting due to configuration deserialization failure.");
            }
        },
        Err(e) => {
            error!("Failed to build config: {:?}", e);
            panic!("Exiting due to configuration builder failure.");
        }
    };

    match move_to_dir(current_folder) {
        Ok(_) => {}
        Err(e) => {
            panic!("Fatal error moving back to current dir: {}", e);
        }
    }

    config
}

#[cfg(test)]
mod tests {
    use crate::config::{load_config, Config};
    use crate::env::{create_tabsdata_home_dir, get_current_dir, get_home_dir};
    use getset::Getters;
    use serde::{Deserialize, Serialize};
    use std::path::PathBuf;

    #[derive(Debug, Clone, Serialize, Deserialize, Getters)]
    #[getset(get = "pub")]
    pub struct MyConfig {
        name: String,
        port: u16,
    }

    impl Default for MyConfig {
        fn default() -> Self {
            Self {
                name: String::from("default_name"),
                port: 1,
            }
        }
    }
    impl Config for MyConfig {}

    #[test]
    fn test_default_as_yaml() {
        println!("{}", MyConfig::default().name());
    }

    #[test]
    #[should_panic]
    fn test_invalid_config_name0() {
        crate::config::assert_config_name("invalid name");
    }

    #[test]
    #[should_panic]
    fn test_invalid_config_name1() {
        crate::config::assert_config_name("invalid_name");
    }

    #[test]
    fn test_load_config_no_files() {
        let config = crate::config::load_config::<MyConfig>("my-config", None, false);
        assert_eq!(config.name(), "default_name");
        assert_eq!(config.port(), &1u16);
    }

    fn write_config_file(file: &PathBuf, config: &MyConfig) {
        std::fs::write(file, config.as_yaml()).expect("Failed to write app config file");
    }

    #[test]
    fn test_load_config_app_file() {
        write_config_file(
            &get_current_dir().join("config").with_extension("yaml"),
            &MyConfig {
                name: "app_config_name".to_string(),
                port: 2,
            },
        );

        let config = load_config::<MyConfig>("my-config", None, false);
        assert_eq!(config.name(), "app_config_name");
        assert_eq!(config.port(), &2u16);
    }

    #[test]
    fn test_load_config_app_file_and_user_app_file() {
        write_config_file(
            &get_current_dir().join("config").with_extension("yaml"),
            &MyConfig {
                name: "app_config_name".to_string(),
                port: 2,
            },
        );
        write_config_file(
            &get_current_dir()
                .join("config_test-user")
                .with_extension("yaml"),
            &MyConfig {
                name: "user_config_name".to_string(),
                port: 3,
            },
        );

        let config = load_config::<MyConfig>("my-config", None, false);
        assert_eq!(config.name(), "user_config_name");
        assert_eq!(config.port(), &3u16);
    }

    #[test]
    fn test_load_config_app_file_user_app_file_and_home_dir() {
        create_tabsdata_home_dir();

        write_config_file(
            &get_current_dir().join("config").with_extension("yaml"),
            &MyConfig {
                name: "app_config_name".to_string(),
                port: 2,
            },
        );
        write_config_file(
            &get_current_dir()
                .join("config_test-user")
                .with_extension("yaml"),
            &MyConfig {
                name: "user_config_name".to_string(),
                port: 3,
            },
        );
        write_config_file(
            &get_home_dir()
                .join(".tabsdata")
                .join("my-config")
                .with_extension("yaml"),
            &MyConfig {
                name: "user_home_dir_config_name".to_string(),
                port: 4,
            },
        );

        let config = load_config::<MyConfig>("my-config", None, false);

        assert_eq!(config.name(), "user_home_dir_config_name");
        assert_eq!(config.port(), &4u16);
    }

    #[test]
    fn test_load_config_app_file_user_app_file_home_dir_and_env() {
        create_tabsdata_home_dir();

        write_config_file(
            &get_current_dir().join("config").with_extension("yaml"),
            &MyConfig {
                name: "app_config_name".to_string(),
                port: 2,
            },
        );
        write_config_file(
            &get_current_dir()
                .join("config_test-user")
                .with_extension("yaml"),
            &MyConfig {
                name: "user_config_name".to_string(),
                port: 3,
            },
        );
        write_config_file(
            &get_home_dir()
                .join(".tabsdata")
                .join("my-config")
                .with_extension("yaml"),
            &MyConfig {
                name: "user_home_dir_config_name".to_string(),
                port: 4,
            },
        );

        std::env::set_var("MY_CONFIG_ENV_NAME", "env_config_name");
        std::env::set_var("MY_CONFIG_ENV_PORT", "5");

        let config = load_config::<MyConfig>("my-config-env", None, false);

        assert_eq!(config.name(), "env_config_name");
        assert_eq!(config.port(), &5u16);
    }
}
