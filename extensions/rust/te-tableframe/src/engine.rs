//
//  Copyright 2024 Tabs Data Inc.
//

use std::io::Error;
use ta_tableframe::api::Extension;

pub struct TableframeExtension;

impl Extension for TableframeExtension {
    fn edition(&self) -> String {
        "standard".to_string()
    }

    fn summary(&self) -> Result<String, Error> {
        Ok("te-tableframe-standard".to_string())
    }
}
