//
// Copyright 2024 Tabs Data Inc.
//

use crate::transporter::api::{AsUrl, Location};
use crate::transporter::error::TransporterError;
use object_store::path::Path;
use object_store::{ObjectStore, parse_url_opts};
use std::collections::HashMap;
use url::Url;

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
