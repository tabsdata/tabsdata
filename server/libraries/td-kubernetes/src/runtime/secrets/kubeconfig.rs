//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;
use std::path::PathBuf;

use async_trait::async_trait;
use tokio::fs;

use crate::base::codec::encode;
use crate::base::constants::{SECRET_KEY_KUBECONFIG, SECRET_TABSDATA_KUBECONFIG};
use crate::base::error::kubernetes::{KubernetesError, kubeconfig::KubeconfigError};
use crate::kube::context::KubeContext;
use crate::runtime::secrets::generator::SecretGenerator;
use crate::runtime::secrets::phase::SecretPhase;

pub struct KubeconfigSecretGenerator {
    kubefile: PathBuf,
    phases: Vec<SecretPhase>,
}

impl KubeconfigSecretGenerator {
    pub fn new(kubefile: PathBuf) -> Self {
        Self {
            kubefile,
            phases: vec![SecretPhase::ClusterStart],
        }
    }
}

#[async_trait]
impl SecretGenerator for KubeconfigSecretGenerator {
    fn name(&self) -> &str {
        SECRET_TABSDATA_KUBECONFIG
    }

    fn phases(&self) -> &[SecretPhase] {
        &self.phases
    }

    async fn generate(
        &self,
        _context: &KubeContext,
    ) -> Result<BTreeMap<String, String>, KubernetesError> {
        let bytes = fs::read(&self.kubefile).await.map_err(|error| {
            KubeconfigError::ReadFailed(
                self.kubefile.to_string_lossy().to_string(),
                error.to_string(),
            )
        })?;
        let mut data = BTreeMap::new();
        data.insert(SECRET_KEY_KUBECONFIG.to_owned(), encode(bytes));
        Ok(data)
    }
}
