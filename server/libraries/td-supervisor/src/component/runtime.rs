//
// Copyright 2025 Tabs Data Inc.
//

//! Module that holds information to setup a runtime environment before running a function on it.

use crate::component::runner::RunnerError;
use crate::component::runner::RunnerError::RuntimeEnvironmentCreationError;
use crate::services::supervisor::RuntimeConfig;
use derive_builder::Builder;
use getset::{Getters, Setters};
use std::collections::HashMap;
use std::path::PathBuf;
use td_python::constants::REQUIREMENTS;
use td_python::venv;

#[derive(Clone, Debug, Getters, Setters, Builder)]
pub struct RuntimeContext {
    variables: HashMap<String, Option<String>>,
}

pub trait RuntimeContextVariables {
    fn variables(&self, envs: Vec<(String, String)>) -> Vec<(String, String)>;
}

impl RuntimeContextVariables for RuntimeContext {
    fn variables(&self, envs: Vec<(String, String)>) -> Vec<(String, String)> {
        let mut envs_map: HashMap<String, String> = envs.into_iter().collect();

        for (key, value) in &self.variables {
            match value {
                Some(value) => {
                    envs_map.insert(key.clone(), value.clone());
                }
                None => {
                    envs_map.remove(key);
                }
            }
        }

        envs_map.into_iter().collect()
    }
}

/// Trait for creating runtime contexts from runtime configurations.
pub trait RuntimeContextProvider {
    fn context(&self, instance: PathBuf, config: PathBuf) -> Result<RuntimeContext, RunnerError>;
}

impl RuntimeContextProvider for RuntimeConfig {
    fn context(&self, instance: PathBuf, config: PathBuf) -> Result<RuntimeContext, RunnerError> {
        match self {
            RuntimeConfig::Java { .. } => Err(RunnerError::UnsupportedRuntime {
                runtime: "Java".to_string(),
            }),
            RuntimeConfig::Node { .. } => Err(RunnerError::UnsupportedRuntime {
                runtime: "Node".to_string(),
            }),
            RuntimeConfig::Python {
                version: _version,
                requirements: _requirements,
            } => {
                let requirements = config.join(REQUIREMENTS);
                let (_environment, variables) =
                    match venv::get(&instance, Some(&requirements), true) {
                        Ok((environment, variables)) => (environment, variables),
                        Err(err) => return Err(RuntimeEnvironmentCreationError(err)),
                    };
                let context = RuntimeContextBuilder::default()
                    .variables(variables)
                    .build()
                    .map_err(|_| RunnerError::UnsupportedRuntime {
                        runtime: "Python".to_string(),
                    })?;
                Ok(context)
            }
        }
    }
}
