//
//  Copyright 2024 Tabs Data Inc.
//
//

use crate::logic::datasets::error::DatasetError;
use td_error::TdError;
use td_objects::datasets::dao::DsFunction;
use td_tower::extractors::Input;

pub async fn upload_function_validate_no_bundle_yet(
    Input(function): Input<DsFunction>,
) -> Result<(), TdError> {
    if *function.bundle_avail() {
        Err(DatasetError::FunctionBundleAlreadyUploaded)?
    } else {
        Ok(())
    }
}
