//
// Copyright 2026 Tabsdata Inc.
//

use std::sync::Arc;

use kube::Client;

#[derive(Clone)]
pub struct KubeContext {
    client: Arc<Client>,
    namespace: String,
}

impl KubeContext {
    pub fn new(client: Arc<Client>, namespace: String) -> Self {
        Self { client, namespace }
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}
