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

/// Create an API server with the given routers and optional layers.
///
/// This macro helps in setting up an API server by combining multiple routers and applying
/// optional middleware layer to them.
#[macro_export]
macro_rules! apiserver {
    ($fn_name:ident {
        $(addresses => $addresses:expr,)?
        $(base_url => $base_url:expr,)?

        $(
            $(#[cfg(feature = $feature:literal)])?
            openapi => $openapi_file:ident,
        )?

        $(
            extension => $extension_file:ident,
        )*

        $(router => { $($router_file:ident => {
            $(config ($($router_config:expr ),*))?
            $(,)?
            $(state ($($router_state:expr ),*))?
        }),* $(,)? }

        $(.layer => $layer:expr)*),* $(,)?}
    $(.layer => $general_layer:expr $(,)?)*
    ) => {
        let $fn_name = async {
            let mut router = axum::Router::new();
            $(
                let mut group_router = axum::Router::new();
                $(
                    group_router = group_router.merge(
                        $router_file::router($($($router_config),*,)? $($($router_state),*)?)
                    );
                )*
                $(
                    group_router = group_router.layer($layer);
                )*
                router = router.merge(group_router);
            )*
            $(
                router = router.layer($general_layer);
            )*
            $(
                router = axum::Router::new().nest($base_url, router);
            )?

            $(
                $(#[cfg(feature = $feature)])? {
                    router = router.merge($openapi_file::router());
                }
            )?

            $(
                router = router.merge($extension_file::router());
            )*

            let mut addresses_if_any = Vec::new();
            $(
                if !$addresses.is_empty() {
                    addresses_if_any = $addresses.clone();
                }
            )?

            $crate::ApiServerBuilder::new(addresses_if_any, router).build().await.unwrap()
        }.await;
    };
}

#[cfg(test)]
mod tests {
    use crate::macros::tests::test::Server;
    use crate::tests::wait_for_server;
    use axum::body::{to_bytes, Body};
    use axum::Router;
    use http::{request, StatusCode};
    use reqwest::Client;
    use tower::ServiceExt;

    mod test {
        use crate::macros::tests::test;
        use crate::{localhost_address, ApiServer};

        #[utoipa::path(get, path = "/test")]
        async fn test_handler() -> &'static str {
            "test"
        }

        router! {
            routes => { test_handler }
        }

        pub struct Server;
        impl Server {
            pub async fn build() -> ApiServer {
                apiserver! {
                    server {
                        addresses => vec![localhost_address(0)],
                        router => {
                            test => {}
                        },
                    }
                }
                server
            }
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

    #[tokio::test]
    async fn test_apiserver_routes() {
        let server = Server::build().await;
        let addr = server.listeners().first().unwrap().local_addr().unwrap();

        tokio::spawn(async move {
            server.run().await;
        });

        let _ = wait_for_server(addr, 100, 10).await;

        let client = Client::new();
        let response = client
            .get(format!("http://{}:{}/test", addr.ip(), addr.port()))
            .send()
            .await
            .expect("Failed to send request");

        assert_eq!(response.status(), 200);

        let body = response.text().await.expect("Failed to read response body");
        assert_eq!(body, "test");
    }
}
