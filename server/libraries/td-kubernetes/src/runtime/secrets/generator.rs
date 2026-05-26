//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::base::error::kubernetes::KubernetesError;
use crate::kube::context::KubeContext;
use crate::runtime::secrets::phase::SecretPhase;

#[async_trait]
pub trait SecretGenerator: Send + Sync {
    fn name(&self) -> &str;

    fn phases(&self) -> &[SecretPhase];

    async fn generate(
        &self,
        context: &KubeContext,
    ) -> Result<BTreeMap<String, String>, KubernetesError>;
}
