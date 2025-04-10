//
//  Copyright 2024 Tabs Data Inc.
//

use tabsdatalib::apiserver;
use tabsdatalib::logic::apiserver::{localhost_address, ApiServer};

use crate::counter::Counter;

/// This example just demonstrate how to create a simple server with a thread safe counter
/// incremented on each request to the `/count` endpoint.

#[tokio::main]
async fn main() {
    let server = init_server().await;
    server.run().await;
}

async fn init_server() -> ApiServer {
    let counter = Counter::create();

    apiserver! {
        simple_server {
            addresses => vec![localhost_address(0)],
            router => {
                endpoint => { state ( counter ) },
            }
        }
    }

    simple_server
}

mod endpoint {
    use crate::counter::CounterState;
    use axum::extract::State;
    use tabsdatalib::router;
    use td_apiforge::status;

    router! {
        state => { CounterState },
        routes => { counter }
    }

    status! {
        CounterStatus,
        OK => usize
    }

    #[utoipa::path(get, path = "/count")]
    pub async fn counter(State(state): State<CounterState>) -> CounterStatus {
        let count = state.lock().await.add();
        CounterStatus::OK(count)
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
        let addr = server.listeners().first().unwrap().local_addr().unwrap();

        tokio::spawn(async {
            server.run().await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let client = Client::new();
        for i in 0..10 {
            let response = client
                .get(format!("http://{}:{}/count", addr.ip(), addr.port()))
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
