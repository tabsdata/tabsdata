//
// Copyright 2026 Tabs Data Inc.
//

//! The local etc-folder catalog: reads interchange files from the instance's
//! `etc/` location (inf-file, `--etc` argument, or the current folder).

use super::EtcError::EtcStoreLocationCreationError;
use super::{EtcContent, EtcError, ServerCatalog};
use async_trait::async_trait;
use pico_args::Arguments;
use std::fs::{File, create_dir_all};
use std::path::PathBuf;
use td_common::env::get_current_dir;
use td_common::manifest::{Inf, WORKER_INF_FILE};
use tokio::{fs, io};
use tracing::warn;

pub const ETC_FOLDER: &str = "etc";
pub const ETC_PARAMETER: &str = "--etc";

#[derive(Debug)]
pub struct EtcFolderStore {
    location: PathBuf,
}

impl EtcFolderStore {
    /// Creates a server catalog instance to read and write interchange files.
    pub async fn new() -> Result<Self, EtcError> {
        // Infers the server catalog base location.
        fn obtain_etc_location() -> PathBuf {
            if let Some(location) = obtain_etc_location_from_info_file() {
                location
            } else if let Some(location) = obtain_etc_location_from_arguments() {
                location
            } else {
                obtain_etc_location_from_current_dir().unwrap()
            }
        }

        // Gets the base server catalog base location form standard inf file.
        fn obtain_etc_location_from_info_file() -> Option<PathBuf> {
            let inf_path = get_current_dir().join(WORKER_INF_FILE);
            if inf_path.exists()
                && let Ok(inf_file) = File::open(&inf_path)
                && let Ok(inf) = serde_yaml::from_reader::<_, Inf>(inf_file)
            {
                return Some(inf.etc);
            }
            None
        }

        // Gets base server catalog base location form passed arguments.
        fn obtain_etc_location_from_arguments() -> Option<PathBuf> {
            let mut arguments = Arguments::from_env();
            let location: Option<PathBuf> =
                arguments.opt_value_from_str(ETC_PARAMETER).unwrap_or(None);
            let _ = arguments.finish();
            location
        }

        // Gets base server catalog base location form current folder.
        fn obtain_etc_location_from_current_dir() -> Option<PathBuf> {
            Some(get_current_dir().join(ETC_FOLDER))
        }

        let location = obtain_etc_location();

        if let Err(e) = create_dir_all(location.clone()) {
            return Err(EtcStoreLocationCreationError { location, cause: e });
        };

        Ok(Self { location })
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_location(location: impl Into<PathBuf>) -> Result<Self, EtcError> {
        Ok(Self {
            location: location.into(),
        })
    }
}

#[async_trait]
impl ServerCatalog for EtcFolderStore {
    async fn read(&self, content: &EtcContent) -> Result<Option<Vec<u8>>, EtcError> {
        let path = self.location.join(content.file());
        match fs::read(&path).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                warn!(
                    "File {} not found as an etc resource location: {}",
                    content.file(),
                    path.display()
                );
                Ok(None)
            }
            Err(e) => Err(EtcError::ReadError {
                location: path,
                cause: e,
            }),
        }
    }
}

#[cfg(test)]
mod tests_etc {
    use super::{EtcContent, EtcFolderStore, ServerCatalog};
    use std::path::Path;

    #[tokio::test]
    async fn test_io_read() {
        let location = td_common::tdtestdir!();
        let path = Path::new(&location).join(EtcContent::ServerBuildManifest_yaml.file());
        let content = "In a hole in the ground there lived a Hobbit.";
        std::fs::write(&path, content).expect("Failed to write the test file.");
        let etc_instance = EtcFolderStore::with_location(&location);
        assert!(
            etc_instance.is_ok(),
            "Failed to create the server catalog instance: {etc_instance:?}"
        );
        let result = etc_instance
            .unwrap()
            .read(&EtcContent::ServerBuildManifest_yaml)
            .await;
        assert!(
            result.is_ok(),
            "Failed to read from the server catalog instance: {result:?}"
        );

        let result_bytes = result.unwrap();
        let result_string =
            String::from_utf8(result_bytes.unwrap()).expect("Failed to convert bytes to string");
        assert_eq!(result_string, content);
    }
}
