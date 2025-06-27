//
// Copyright 2025 Tabs Data Inc.
//

/// Create an Axum router using OpenApi router with the given routes.
#[macro_export]
macro_rules! router {
    {
        $(
            state => { $( $state:ident ),* $(,)? },
        )?
        routes => { $( $handler:ident ),* $(,)? }
    }
     => {
        paste::paste! {
            pub fn router(
                $( $([< $state:snake >]: $state,)* )?
            ) -> utoipa_axum::router::OpenApiRouter {
                let router = utoipa_axum::router::OpenApiRouter::new()
                $(
                    .routes(utoipa_axum::routes!($handler))
                )*;
                router$( .with_state(($([< $state:snake >]),*)) )?
            }
        }
    };
}

/// Create a router with the given routes and optional layers.
#[macro_export]
macro_rules! routers {
    (
        $(config => { $config:ident },)?
        $(state => { $( $state:ident ),* $(,)? },)?
        $(router => { $($router_file:ident => {
            $(config ($($router_config:expr ),*))?
            $(,)?
            $(state ($($router_state:expr ),*))?
        }),* $(,)? }
        $(.layer => $layer:expr)*),* $(,)?
    ) => {
        #[allow(non_snake_case)]
        pub fn router(
            $( $config: $config, )?
            $( $($state: $state,)* )?
        ) -> axum::Router {
            let mut router = axum::Router::new();
            $(
                let mut group_router = axum::Router::new();
                $( group_router = group_router.merge(
                    $router_file::router($($($router_config.clone()),*,)? $($($router_state.clone()),*)?)
                ); )*
                $( group_router = group_router.layer($layer); )*
                router = router.merge(group_router);
            )*
            router
        }
    };
}

#[cfg(test)]
mod tests {
    use axum::body::{to_bytes, Body};
    use axum::Router;
    use http::{request, StatusCode};
    use tower::ServiceExt;

    mod test {
        #[utoipa::path(get, path = "/test")]
        async fn test_handler() -> &'static str {
            "test"
        }

        router! {
            routes => { test_handler }
        }
    }

    #[tokio::test]
    async fn test_router() {
        let router: Router = test::router().into();

        let response = router
            .oneshot(
                request::Request::builder()
                    .uri("/test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body, "test");
    }
}
