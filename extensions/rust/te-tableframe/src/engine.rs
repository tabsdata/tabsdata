//
//  Copyright 2024 Tabs Data Inc.
//

use std::io::Error;
use ta_tableframe::api::{Extension, OPEN_SOURCE};

pub struct TableFrameExtension;

impl Extension for TableFrameExtension {
    fn edition(&self) -> String {
        OPEN_SOURCE.to_string()
    }

    fn summary(&self) -> Result<String, Error> {
        Ok("te-tableframe-open-source".to_string())
    }
}
