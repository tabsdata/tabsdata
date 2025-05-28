//
//  Copyright 2024 Tabs Data Inc.
//

#[macro_export]
macro_rules! layers {
    ($($layer:expr),* $(,)? $(; map_err = $map_err:expr)?) => {
        $crate::box_sync_clone_layer::BoxedSyncCloneServiceLayer::boxed_layer(
            tower::builder::ServiceBuilder::new()
                $(.map_err($map_err))?
                $(
                    .layer($layer)
                )*
        )
    };
}

#[macro_export]
macro_rules! service {
    ($($layers:expr),* $(,)?) => {
        tower::builder::ServiceBuilder::new()
            $(.layer($layers))*
            .map_err(td_error::TdError::from)
            .service($crate::default_services::ServiceReturn)
    };
}

#[macro_export]
macro_rules! service_provider {
    ($($layers:expr),* $(,)?) => {
        tower::builder::ServiceBuilder::new()
            .layer($crate::default_services::ServiceEntry::default())
            $(.layer($layers))*
            .map_err(td_error::TdError::from)
            .service($crate::default_services::ServiceReturn)
            .into_service_provider()
    };
}

#[macro_export]
macro_rules! p {
    {
        $fn_name:ident($( $arg:ident: $arg_type:ty),* $(,)?)
    {
        $content:expr
    } } => {
        fn $fn_name<Req, Res>(
            $( $arg: $arg_type, )*
        ) -> $crate::service_provider::ServiceProvider<Req, Res, td_error::TdError>
        where
            Req: $crate::default_services::Share,
            Res: $crate::default_services::Share,
        {
            $content
        }
    };
}

#[macro_export]
macro_rules! l {
    {
        $fn_name:ident($( $arg:ident: $arg_type:ty),* $(,)?)
    {
        $content:expr
    } } => {
        pub fn $fn_name<In>($( $arg: $arg_type, )*)
        -> $crate::box_sync_clone_layer::BoxSyncCloneLayer<
                In,
                $crate::handler::Handler,
                $crate::handler::Handler,
                td_error::TdError>
        where
            In: tower::Service<
                $crate::handler::Handler,
                Response = $crate::handler::Handler,
                Error = td_error::TdError,
                Future: Send>
            + Clone + $crate::default_services::Share,
        {
            $content
        }
    };
}

#[cfg(test)]
mod tests {
    use crate::from_fn::from_fn;
    use crate::service_provider::{IntoServiceProvider, ServiceProvider};
    use td_error::TdError;
    use tower::layer::util::Identity;
    use tower_service::Service;

    async fn layer_fn() -> Result<(), TdError> {
        Ok(())
    }

    #[test]
    fn test_service_macro() {
        let layers = layers!(Identity::new());
        let _ = service!(layers);
    }

    #[test]
    fn test_service_provider_macro() {
        let layers = layers!(Identity::new());
        let _: ServiceProvider<(), (), TdError> = service_provider!(layers);
    }

    #[tokio::test]
    async fn test_p_macro_no_args() {
        p! {
            test_provider() {
                service_provider!(layers!(Identity::new()))
            }
        }
        let _: () = *test_provider().make().await.call(()).await.unwrap();
    }

    #[tokio::test]
    async fn test_p_macro_single_arg() {
        p! {
            test_provider(_arg: u32) {
                service_provider!(layers!(Identity::new()))
            }
        }
        let _: () = *test_provider(1).make().await.call(()).await.unwrap();
    }

    #[tokio::test]
    async fn test_p_macro_multiple_arg() {
        p! {
            test_provider(_arg_1: u32, _arg_2: &str) {
                service_provider!(layers!(Identity::new()))
            }
        }
        let _: () = *test_provider(42, "test")
            .make()
            .await
            .call(())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_l_macro() {
        l! {
            test_layers() {
                layers!(
                    Identity::new(),
                    from_fn(layer_fn),
                )
            }
        }

        l! {
            test_layers_with_args(_arg_1: u32, _arg_2: &str) {
                layers!(
                    Identity::new(),
                    from_fn(layer_fn),
                )
            }
        }

        p! {
            test_provider(arg_1: u32, arg_2: &str) {
                service_provider!(layers!(
                    Identity::new(),
                    test_layers(),
                    test_layers_with_args(arg_1, arg_2)
                ))
            }
        }

        let _: () = *test_provider(42, "test")
            .make()
            .await
            .call(())
            .await
            .unwrap();
    }
}
