//
// Copyright 2026 Tabs Data Inc.
//

use std::io::Read;

use td_common::status::ExitStatus::GeneralError;

use crate::cerror;
use crate::instance::profile::LoadedProfile;
use crate::provider::Provider;
use crate::system::exit::{Result, abort};

const INIT_PHASE_PREFIX: &str = "pods/init/";

pub async fn apply_init_phase(
    provider: &dyn Provider,
    loaded: &LoadedProfile,
) -> Result {
    for path in loaded.manifest.entries.keys() {
        if !path.starts_with(INIT_PHASE_PREFIX) {
            continue;
        }
        let yaml = read_manifest(loaded, path)?;
        provider.apply_init_resource(path, &yaml).await?;
    }
    Ok(())
}

fn read_manifest(loaded: &LoadedProfile, path: &str) -> Result<String> {
    let target = match loaded.tree.join(path) {
        Ok(target) => target,
        Err(error) => {
            cerror!("Failed to resolve virtual path '{path}': {error}");
            return abort(GeneralError);
        }
    };
    let mut reader = match target.open_file() {
        Ok(reader) => reader,
        Err(error) => {
            cerror!(
                "Failed to open virtual file '{}': {error}",
                target.as_str()
            );
            return abort(GeneralError);
        }
    };
    let mut yaml = String::new();
    if let Err(error) = reader.read_to_string(&mut yaml) {
        cerror!(
            "Failed to read virtual file '{}' as UTF-8: {error}",
            target.as_str()
        );
        return abort(GeneralError);
    }
    Ok(yaml)
}
