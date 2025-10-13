//
//  Copyright 2024 Tabs Data Inc.
//

use crate::counter::Counter;
use crate::endpoint::CounterRouter;
use std::net::{Ipv4Addr, SocketAddr};
use ta_apiserver::router::RouterExtension;
use td_apiserver::{Server, ServerBuilder};
use td_objects::types::basic::NonEmptyAddresses;
use td_process::launcher::hooks;

/// This example just demonstrate how to create a simple server with a thread safe counter
/// incremented on each request to the `/count` endpoint.

#[tokio::main]
async fn main() {
    hooks::panic();

    let server = init_server().await;
    server.run().await.unwrap();
}

async fn init_server() -> Box<dyn Server> {
    let counter = Counter::create();
    ServerBuilder::new(
        NonEmptyAddresses::from_vec(vec![SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0)]).unwrap(),
        CounterRouter::router(counter).into(),
    )
    .build()
    .await
    .unwrap()
}

mod endpoint {
    use td_apiforge::router_ext;

    #[router_ext(CounterRouter)]
    mod routes {
        use crate::counter::CounterState;
        use axum::extract::State;
        use ta_apiserver::status::error_status::ErrorStatus;
        use ta_apiserver::status::ok_status::RawStatus;
        use td_apiforge::apiserver_path;

        const PATH: &str = "/count";
        const TEST_TAG: &str = "Test";

        #[apiserver_path(method = get, path = PATH, tag = TEST_TAG)]
        pub async fn counter(
            State(state): State<CounterState>,
        ) -> Result<RawStatus<usize>, ErrorStatus> {
            let count = state.lock().await.add();
            Ok(RawStatus::OK(count))
        }
    }
}

mod counter {
    use std::sync::Arc;
    use tokio::sync::Mutex;

    pub type CounterState = Arc<Mutex<Counter>>;

    pub struct Counter {
        count: usize,
    }

    impl Counter {
        pub fn create() -> CounterState {
            Arc::new(Mutex::new(Counter { count: 0 }))
        }

        pub fn add(&mut self) -> usize {
            self.count += 1;
            self.count
        }
    }
}

#[cfg(test)]
mod tests {
    use reqwest::Client;

    #[tokio::test]
    async fn test_simple_server() {
        let server = super::init_server().await;
        let addr = server.listeners()[0].local_addr().unwrap();
        let scheme = server.scheme();

        tokio::spawn(async {
            server.run().await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let client = Client::new();
        for i in 0..10 {
            let response = client
                .get(format!("{}://{}:{}/count", scheme, addr.ip(), addr.port()))
                .send()
                .await
                .expect("Failed to send request");

            assert_eq!(response.status(), 200);

            let body = response.text().await.expect("Failed to read response body");
            let count: usize = body.parse().unwrap();
            assert_eq!(count, i + 1);
        }
    }
}
