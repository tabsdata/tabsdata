//
// Copyright 2024 Tabs Data Inc.
//

use crate::transporter::api::{AsUrl, Location};
use crate::transporter::error::TransporterError;
use object_store::path::Path;
use object_store::{parse_url_opts, ObjectStore};

pub fn create_store<L>(
    location: &Location<L>,
) -> Result<(Box<dyn ObjectStore>, Path), TransporterError>
where
    L: AsUrl,
{
    let url = location.url();

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

    let parsed = parse_url_opts(&url, location.cloud_configs())
        .map_err(|err| TransporterError::CouldNotCreateObjectStore(url.to_string(), err))?;

    let (store, parsed_path) = parsed;
    let path = if url.scheme() == "az" {
        let path = url.path().strip_prefix('/').unwrap_or("");
        Path::from(path)
    } else {
        parsed_path
    };

    Ok((store, path))
}
