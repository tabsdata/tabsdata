//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_objects::datasets::dto::*;
use td_objects::dlo::CollectionName;
use td_tower::extractors::Input;

pub async fn upload_function_to_collection_name(
    Input(upload_function): Input<UploadFunction>,
) -> Result<CollectionName, TdError> {
    Ok(CollectionName::new(upload_function.collection()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::Request;
    use td_objects::rest_urls::FunctionIdParam;

    #[tokio::test]
    async fn test_upload_function_to_collection_name() {
        let payload = "TEST";
        let request = Request::builder()
            .body(Body::new(payload.to_string()))
            .unwrap();
        let upload_function = UploadFunction::new(FunctionIdParam::new("ds", "d", "f"), request);

        let upload_function = Input::new(upload_function);
        let res = upload_function_to_collection_name(upload_function).await;
        assert_eq!(res.unwrap().as_ref(), "ds");
    }
}
