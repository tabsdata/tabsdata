//
// Copyright 2026 Tabsdata Inc.
//

use crate::base::constants::{
    ENV_PIP_INDEX_URL, ENV_UV_DEFAULT_INDEX, ENV_UV_INDEX, ENV_UV_INDEX_STRATEGY, PYPI_INDEX_URL,
    UV_INDEX_STRATEGY_BEST_MATCH, UvMode, environment_index,
};
use crate::runtime::instance::tabsdata::{EnvValue, TabsdataConfig};
use crate::runtime::provider::ClusterProvider;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexOrigin {
    Argument,
    UvIndex,
    PipIndexUrl,
    Absent,
}

impl IndexOrigin {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Argument => "--uv-index",
            Self::UvIndex => ENV_UV_INDEX,
            Self::PipIndexUrl => ENV_PIP_INDEX_URL,
            Self::Absent => "<unset>",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexPlan {
    pub local: Option<String>,
    pub origin: IndexOrigin,
    pub remote: Option<String>,
}

impl IndexPlan {
    pub fn same(&self) -> bool {
        self.local == self.remote
    }

    pub fn none(&self) -> bool {
        self.local.is_none() && self.remote.is_some()
    }

    pub fn obtain_local(&self) -> String {
        match &self.local {
            Some(index) => format!("{index} (from {})", self.origin.as_str()),
            None => "<none>".to_owned(),
        }
    }

    pub fn obtain_remote(&self) -> String {
        self.remote.clone().unwrap_or_else(|| "<none>".to_owned())
    }
}

pub fn governed(provider: ClusterProvider, mode: UvMode) -> bool {
    provider == ClusterProvider::Kminus && mode == UvMode::Unset
}

pub fn resolve(argument: Option<&str>, remote: Option<String>) -> IndexPlan {
    let (local, origin) = match argument
        .map(str::trim)
        .filter(|index| !index.is_empty())
        .map(str::to_owned)
    {
        Some(index) => (Some(index), IndexOrigin::Argument),
        None => match environment_index(ENV_UV_INDEX) {
            Some(index) => (Some(index), IndexOrigin::UvIndex),
            None => match environment_index(ENV_PIP_INDEX_URL) {
                Some(index) => (Some(index), IndexOrigin::PipIndexUrl),
                None => (None, IndexOrigin::Absent),
            },
        },
    };
    IndexPlan {
        local,
        origin,
        remote,
    }
}

pub fn remote_index(config: &TabsdataConfig) -> Option<String> {
    config
        .pods
        .global
        .envs
        .get(ENV_UV_INDEX)
        .and_then(EnvValue::render)
        .map(|index| index.trim().to_owned())
        .filter(|index| !index.is_empty())
}

pub fn apply(config: &mut TabsdataConfig, local: Option<&str>) {
    let envs = &mut config.pods.global.envs;
    match local {
        Some(index) => {
            envs.insert(ENV_UV_INDEX.to_owned(), EnvValue::Text(index.to_owned()));
        }
        None => {
            envs.insert(ENV_UV_INDEX.to_owned(), EnvValue::Unset(()));
        }
    }
    envs.insert(
        ENV_UV_DEFAULT_INDEX.to_owned(),
        EnvValue::Text(PYPI_INDEX_URL.to_owned()),
    );
    envs.insert(
        ENV_UV_INDEX_STRATEGY.to_owned(),
        EnvValue::Text(UV_INDEX_STRATEGY_BEST_MATCH.to_owned()),
    );
}

#[cfg(test)]
mod tests;
