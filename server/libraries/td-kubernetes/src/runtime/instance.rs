//
// Copyright 2026 Tabs Data Inc.
//

pub mod launcher;
pub mod manifest;
pub mod profile;
pub mod tier;

use td_common::status::ExitStatus::TabsDataError;
use td_python::tabsdata::tabsdata_version;

use crate::cerror;
use crate::core::system::exit::{Result, abort};
use crate::runtime::instance::manifest::{VersionMismatch, check_versions, deserialize_manifest};

pub fn verify_manifest_compatibility(manifest_yaml: &str) -> Result {
    let manifest = match deserialize_manifest(manifest_yaml) {
        Ok(manifest) => manifest,
        Err(error) => {
            cerror!("Failed to parse profile manifest: {error}");
            return abort(TabsDataError);
        }
    };
    let running = match tabsdata_version() {
        Ok(version) => version,
        Err(error) => {
            cerror!("Failed to resolve the tabsdata package version: {error}");
            return abort(TabsDataError);
        }
    };
    match check_versions(&manifest, &running) {
        Ok(()) => Ok(()),
        Err(VersionMismatch::Parse { value, source }) => {
            cerror!("Failed to parse manifest version '{value}': {source}");
            abort(TabsDataError)
        }
        Err(VersionMismatch::Mismatch { running, entries }) => {
            cerror!(
                "Profile is incompatible with the running tabsdata SDK '{running}'. \
                 The following entries differ on major.minor:"
            );
            for entry in &entries {
                cerror!("  - {} (at version {})", entry.path, entry.version);
            }
            cerror!("Run 'tdkserver upgrade <instance>' first.");
            abort(TabsDataError)
        }
    }
}
