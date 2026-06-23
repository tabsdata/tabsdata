//
// Copyright 2026 Tabs Data Inc.
//

use std::fs;
use std::path::Path;

use crate::base::error::gateway::ResolverError;

pub fn fuse(init: &Path, stage: &Path) -> Result<(), ResolverError> {
    let _ = init;
    wipe_stage(stage)?;
    Ok(())
}

fn wipe_stage(stage: &Path) -> Result<(), ResolverError> {
    if !stage.exists() {
        return Ok(());
    }
    fs::remove_dir_all(stage).map_err(|cause| ResolverError::StageWipeFailed {
        path: stage.display().to_string(),
        cause: cause.to_string(),
    })
}
