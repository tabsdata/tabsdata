//
// Copyright 2025 Tabs Data Inc.
//

use crate::component::describer::DescriberError;
use crate::resource::instance::{
    create_instance_tree, get_instance_path_for_instance, get_repository_path_for_instance,
    get_workspace_path_for_instance, InstanceError,
};
use crate::runtime::error::RuntimeError;
use crate::services::bootloader::BootloaderError::*;
use clap::Parser;
use getset::Getters;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::io::Write;
use std::path::PathBuf;
use std::{fs, io};
use td_build::version::TABSDATA_VERSION;
use td_common::cli::Cli;
use td_common::config::Config;
use td_common::server::{CONFIG_FOLDER, WORK_FOLDER};
use td_common::status::ExitStatus;
use td_common::status::ExitStatus::*;
use thiserror::Error;
use tracing::{error, info};

pub const BOOTLOADER: &str = "bootloader";

pub const BOOTLOADER_ARGUMENT_INSTANCE: &str = "--instance";
pub const BOOTLOADER_ARGUMENT_REPOSITORY: &str = "--repository";
pub const BOOTLOADER_ARGUMENT_WORKSPACE: &str = "--workspace";
pub const BOOTLOADER_ARGUMENT_PROFILE: &str = "--profile";

const VERSION: &str = ".version";

#[derive(Default, Debug, Clone, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct Configuration {
    instance: Option<PathBuf>,
    repository: Option<PathBuf>,
    workspace: Option<PathBuf>,
    profile: Option<PathBuf>,
    config: Option<PathBuf>,
    work: Option<PathBuf>,
}

impl Config for Configuration {}

#[derive(Debug, Clone, clap_derive::Parser)]
#[command(
    name = "Tabsdata Boot Loader",
    version = "0.1.0",
    about = "Tabsdata Boot Loader",
    long_about = "Tabsdata's bootloader prepares the execution instance resources."
)]
pub struct Arguments {
    /// Name/Location of the Tabsdata instance.
    #[arg(
        long,
        name = "instance",
        default_value = None,
        required = true,
        value_parser = parse_instance,
        long_help = "Name/Location of the Tabsdata instance. \
                     The instance is stored as a subfolder of the user's home folder, when a relative path. \
                     If unspecified, instance ~/.tabsdata/instances/tabsdata will be used."
    )]
    instance: Option<PathBuf>,

    /// Folder containing the instance's persistent data.
    #[arg(
        long,
        name = "repository",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Folder containing the instance's persistent data. \
                     If unspecified, the subfolder 'repository' inside the instance's folder will be used."
    )]
    repository: Option<PathBuf>,

    /// Folder containing the instance's transient data.
    #[arg(
        long,
        name = "workspace",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Folder containing the instance's transient data. \
                     If unspecified, the subfolder 'workspace' inside the instance's folder will be used."
    )]
    workspace: Option<PathBuf>,

    /// Folder containing the instance's profile.
    #[arg(
        long,
        name = "profile",
        required = false,
        default_value = None,
        value_parser = parse_profile,
        long_help = "Folder containing the instance's profile. \
                     The default Tabsdata profile will we used if unspecified."
    )]
    profile: Option<PathBuf>,
}

impl Arguments {
    pub fn show(&self, config: &Configuration) -> String {
        fn path_or_none(value: Option<PathBuf>) -> String {
            value.map_or_else(|| "<None>".to_string(), |v| v.to_string_lossy().to_string())
        }
        let mut output = String::new();
        let params_to_show = [
            ("instance", Some(self.instance(config))),
            ("repository", Some(self.repository(config))),
            ("workspace", Some(self.workspace(config))),
            ("profile", self.profile(config)),
            ("conf", Some(self.config(config))),
            ("work", Some(self.work(config))),
        ];
        for (name, path) in params_to_show {
            output.push_str(&format!("Using {}: '{}'\n", name, path_or_none(path)));
        }
        output
    }
}

fn parse_profile(profile: &str) -> Result<PathBuf, BootloaderError> {
    if profile.trim().is_empty() {
        return Err(UnspecifiedCustomProfile);
    };
    if profile.trim() != profile {
        return Err(EdgeSpacesInCustomProfile {
            profile: profile.to_string(),
        });
    };
    let profile_path: Result<PathBuf, _> =
        profile
            .parse::<PathBuf>()
            .map_err(|_err: Infallible| InvalidCustomProfile {
                profile: profile.to_string(),
            });
    match profile_path {
        Ok(profile_path) => {
            if !profile_path.exists() {
                return Err(NonExistingCustomProfile {
                    profile: profile_path.clone(),
                });
            }
            Ok(profile_path)
        }
        Err(e) => Err(e),
    }
}

fn parse_instance(instance: &str) -> Result<PathBuf, BootloaderError> {
    if instance.trim().is_empty() {
        return Err(UnspecifiedInstance);
    };
    if instance.trim() != instance {
        return Err(EdgeSpacesInInstance {
            instance: instance.to_string(),
        });
    };
    let instance_path: Result<PathBuf, _> =
        instance
            .parse::<PathBuf>()
            .map_err(|_err: Infallible| InvalidInstance {
                instance: instance.to_string(),
            });
    instance_path
}

fn value(arg: &Option<PathBuf>, config: &Option<PathBuf>) -> Option<PathBuf> {
    arg.as_ref().or(config.as_ref()).cloned()
}

impl Arguments {
    fn profile(&self, config: &Configuration) -> Option<PathBuf> {
        value(&self.profile, &config.profile)
    }

    fn instance(&self, config: &Configuration) -> PathBuf {
        get_instance_path_for_instance(&value(&self.instance, config.instance()))
    }

    fn repository(&self, config: &Configuration) -> PathBuf {
        get_repository_path_for_instance(&Some(self.instance(config)))
    }

    fn workspace(&self, config: &Configuration) -> PathBuf {
        get_workspace_path_for_instance(&Some(self.instance(config)))
    }

    fn config(&self, config: &Configuration) -> PathBuf {
        get_workspace_path_for_instance(&Some(self.instance(config))).join(CONFIG_FOLDER)
    }

    fn work(&self, config: &Configuration) -> PathBuf {
        get_workspace_path_for_instance(&Some(self.instance(config))).join(WORK_FOLDER)
    }
}

#[derive(Default)]
pub struct Bootloader {}

impl Bootloader {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Bootloader {
    async fn run(
        &self,
        _config: Configuration,
        _params: Arguments,
    ) -> Result<ExitStatus, RuntimeError> {
        let config = Configuration::default();
        let params = Arguments::parse();
        info!("{}", params.show(&config));
        match self.init_instance(&config, &params) {
            Ok(_) => Ok(Success),
            Err(e) => {
                error!("Tabsdata bootloader initialization failed: {}", e);
                error!("Leaving with exit code: {}", TabsDataError.code());
                Ok(TabsDataError)
            }
        }
    }

    fn init_instance(
        &self,
        config: &Configuration,
        params: &Arguments,
    ) -> Result<(), RuntimeError> {
        match create_instance_tree(params.profile(config), Some(params.instance(config))) {
            Ok(_) => Ok(()),
            Err(e) => Err(RuntimeError::new(e.to_string())),
        }?;
        match self.stamp_instance(config, params) {
            Ok(_) => Ok(()),
            Err(e) => Err(RuntimeError::new(e.to_string())),
        }
    }

    fn stamp_instance(&self, config: &Configuration, params: &Arguments) -> io::Result<()> {
        let version = params.instance(config).join(VERSION);
        if !version.exists() {
            let mut file = fs::File::create(&version)?;
            file.write_all(TABSDATA_VERSION.trim().as_bytes())?;
        };
        Ok(())
    }
}

pub fn start() {
    Cli::<Configuration, Arguments>::exec_async(
        BOOTLOADER,
        |config, params| async move {
            let result = Bootloader::new().run(config, params).await;
            if let Err(e) = result {
                error!("Tabsdata bootloader execution failed: {}", e);
                error!("Leaving with exit code: {}", TabsDataError.code());
                return TabsDataError;
            }
            Success
        },
        None,
    );
}

#[derive(Debug, Error)]
pub enum BootloaderError {
    #[error("Invalid custom profile specified: {profile}")]
    InvalidCustomProfile { profile: String },
    #[error("The custom profile has leading or trailing spaces: {profile}")]
    EdgeSpacesInCustomProfile { profile: String },
    #[error("Custom profile not specified")]
    UnspecifiedCustomProfile,
    #[error("Non existing custom profile: {profile}")]
    NonExistingCustomProfile { profile: PathBuf },
    #[error("Invalid instance specified: {instance}")]
    InvalidInstance { instance: String },
    #[error("The instance has leading or trailing spaces: {instance}")]
    EdgeSpacesInInstance { instance: String },
    #[error("Instance not specified")]
    UnspecifiedInstance,
    #[error("Failed to start Tabsdata main worker '{command}': {cause}")]
    LaunchError { command: String, cause: io::Error },
    #[error("An error occurred at instance level: {0}")]
    InstanceFailure(#[from] InstanceError),
    #[error("Failed to describe the Tabsdata main worker '{command}': {cause}")]
    DescribeFailure {
        command: String,
        cause: DescriberError,
    },
}
