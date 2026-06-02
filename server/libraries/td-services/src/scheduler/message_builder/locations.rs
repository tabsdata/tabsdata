//
// Copyright 2026 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::dxo::yaml::location::Location;
use td_objects::types::basic::EnvPrefix;
use td_storage::{SPath, Storage};

/// Resolve `path` through `storage` and wrap it into the worker-facing
/// `Location`, returning the `EnvPrefix` separately so callers can register
/// it in their `get_states` set when needed.
pub(crate) fn build_external_location(
    storage: &Storage,
    path: &SPath,
) -> Result<(Location, EnvPrefix), TdError> {
    let (external_path, mount_def) = storage.to_external_uri(path)?;
    let env_prefix = EnvPrefix::try_from(&mount_def.id)?;
    let location = Location {
        uri: external_path,
        env_prefix: Some(env_prefix.clone()),
    };
    Ok((location, env_prefix))
}
