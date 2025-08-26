//
//  Copyright 2024 Tabs Data Inc.
//

#[macro_export]
macro_rules! layers {
    ($($layer:expr_2021),* $(,)?) => {
        $crate::box_sync_clone_layer::BoxedSyncCloneServiceLayer::boxed_layer(
            tower::builder::ServiceBuilder::new()
                $(
                    .layer($layer)
                )*
        )
    };
}

#[macro_export]
macro_rules! service {
    ($($layers:expr_2021),* $(,)?) => {
        tower::builder::ServiceBuilder::new()
            $(.layer($layers))*
            .map_err(td_error::TdError::from)
            .service($crate::default_services::ServiceReturn)
    };
}

#[cfg(test)]
mod tests {
    use crate::from_fn::from_fn;
    use td_error::TdError;
    use tower::layer::util::Identity;

    async fn layer_fn() -> Result<(), TdError> {
        Ok(())
    }

    #[test]
    fn test_macros() {
        let layers = layers!(Identity::new(), from_fn(layer_fn));
        let _ = service!(layers);
    }
}
