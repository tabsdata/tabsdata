//
// Copyright 2026 Tabs Data Inc.
//

use tracing::{debug, info};

use crate::base::error::kubernetes::KubernetesError;
use crate::kube::api::secret::upsert_secret;
use crate::kube::context::KubeContext;
use crate::runtime::secrets::generator::SecretGenerator;
use crate::runtime::secrets::phase::SecretPhase;

pub struct SecretHandler;

impl SecretHandler {
    pub async fn handle(
        context: &KubeContext,
        phase: SecretPhase,
        generators: &[Box<dyn SecretGenerator>],
    ) -> Result<(), KubernetesError> {
        for generator in generators {
            if !generator.phases().contains(&phase) {
                debug!(
                    secret = %generator.name(),
                    phase = %phase,
                    "secret.skip: generator not registered for phase"
                );
                continue;
            }
            let data = generator.generate(context).await?;
            upsert_secret(context, generator.name(), data, None).await?;
            info!(
                secret = %generator.name(),
                namespace = %context.namespace(),
                phase = %phase,
                "secret.handle: completed"
            );
        }
        Ok(())
    }
}
