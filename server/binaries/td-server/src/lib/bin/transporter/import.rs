//
// Copyright 2024 Tabs Data Inc.
//

use crate::bin::transporter::api::{ErrorReport, ImportReport, ImportRequest};

pub async fn import(_request: ImportRequest) -> Result<ImportReport, ErrorReport> {
    //TODO: migrate importer logic here
    println!("Importing data is not yet implemented");
    Ok(ImportReport::new(vec![]))
}
