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
    parse_url_opts(&url, location.cloud_configs())
        .map_err(|err| TransporterError::CouldNotCreateObjectStore(url.to_string(), err))
}
