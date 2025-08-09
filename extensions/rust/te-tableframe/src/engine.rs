//
//  Copyright 2024 Tabs Data Inc.
//

use std::io::Error;
use ta_tableframe::api::Extension;

pub struct TableFrameExtension;

impl Extension for TableFrameExtension {
    fn edition(&self) -> String {
        "Open Source".to_string()
    }

    fn summary(&self) -> Result<String, Error> {
        Ok("te-tableframe-opens-source".to_string())
    }
}
