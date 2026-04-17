//
// Copyright 2024 Tabs Data Inc.
//

use crate::transporter::api::{AsUrl, Location};
use crate::transporter::error::TransporterError;
use object_store::path::Path;
use object_store::{ObjectStore, parse_url_opts};
use std::collections::HashMap;
use url::Url;

/// Extract the file system root URL from a file:// URL.
///
/// On Unix returns `file:///`. On Windows extracts the drive letter from the
/// URL path (e.g. `file:///C:/foo` → `file:///C:/`) to preserve the original
/// case and avoid mismatches with case-sensitive `strip_prefix` or `Url::join`
/// interpreting the drive letter as a URL scheme.
pub fn file_root_url(url: &Url) -> Url {
    let mut root = url.clone();
    let path = url.path();
    let bytes = path.as_bytes();
    // On Windows, file:// URLs have paths like /C:/some/path — keep /C:/
    if bytes.len() >= 4
        && bytes[0] == b'/'
        && bytes[1].is_ascii_alphabetic()
        && bytes[2] == b':'
        && bytes[3] == b'/'
    {
        root.set_path(&path[..4]);
    } else {
        root.set_path("/");
    }
    root
}

pub fn create_store<L>(
    location: &Location<L>,
) -> Result<(Box<dyn ObjectStore>, Path), TransporterError>
where
    L: AsUrl,
{
    let (store, path) = parse_store(&location.url(), &location.cloud_configs())?;
    let path = tweak_store(&location.url(), &path);
    Ok((store, path))
}

pub fn parse_store(
    url: &Url,
    cloud_configs: &HashMap<String, String>,
) -> Result<(Box<dyn ObjectStore>, Path), TransporterError> {
    parse_url_opts(url, cloud_configs)
        .map_err(|err| TransporterError::CouldNotCreateObjectStore(url.to_string(), err))
}

// The commit https://github.com/apache/arrow-rs-object-store/commit/f422dce1528ee2a089d8061af639c3f2a9cd43af
// broke our code, as it parses urls like: az://tabsdataci/test_output/test_output_azure_parquet_1753806788.parquet
// as:
//
//    - az -> Scheme (correct)
//    - tabsdataci -> Host (incorrect)
//    - test_output -> Container/Bucket (incorrect)
//    - test_output_azure_parquet_1753806788.parquet -> Path (incorrect)
//
// when it should be:
//
//    - az -> Scheme
//    - tabsdataci -> Container/Bucket
//    - test_output/test_output_azure_parquet_1753806788.parquet -> Path
pub fn tweak_store(url: &Url, path: &Path) -> Path {
    if url.scheme() == "az" {
        let path = url.path().strip_prefix('/').unwrap_or("");
        Path::from(path)
    } else {
        path.clone()
    }
}
