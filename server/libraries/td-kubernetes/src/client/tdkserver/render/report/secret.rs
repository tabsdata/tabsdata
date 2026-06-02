//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeSet;

use tracing::{info, warn};

use crate::base::error::secret::SecretError;
use crate::kube::api::secret::get_secret;
use crate::kube::context::KubeContext;

pub struct ReportInput<'a> {
    pub secret_name: &'a str,
    pub expected_keys: &'a [String],
}

pub enum ReportContext<'a> {
    Live(&'a KubeContext),
    ClusterDown,
}

pub async fn reconciliation(
    context: &ReportContext<'_>,
    input: &ReportInput<'_>,
) -> Result<(), SecretError> {
    info!("{}:", input.secret_name);
    let kube_context = match context {
        ReportContext::ClusterDown => {
            warn!(
                "  - Reconciliation check skipped: the cluster is stopped. \
                 Start the cluster and rerun `tdkserver secret get` to verify deployed Secrets."
            );
            return Ok(());
        }
        ReportContext::Live(ctx) => ctx,
    };

    let live = get_secret(kube_context, input.secret_name).await?;
    let Some(live) = live else {
        warn!("  - {}: Not deployed.", label("Status"));
        info!("  - {}: N/A.", label("Extra keys"));
        info!("  - {}: N/A.", label("Missing keys"));
        info!("  - {}: N/A.", label("Empty values"));
        return Ok(());
    };

    info!("  - {}: Deployed.", label("Status"));

    let expected: BTreeSet<&str> = input.expected_keys.iter().map(String::as_str).collect();
    let live_keys: BTreeSet<&str> = live.keys().map(String::as_str).collect();

    let extra: Vec<&str> = live_keys.difference(&expected).copied().collect();
    let missing: Vec<&str> = expected.difference(&live_keys).copied().collect();
    let empty: Vec<&str> = live
        .iter()
        .filter(|(_, value)| trimmed_is_empty(value))
        .map(|(key, _)| key.as_str())
        .collect();

    info!("  - {}: {}.", label("Extra keys"), render_list(&extra));
    if missing.is_empty() {
        info!("  - {}: None.", label("Missing keys"));
    } else {
        warn!(
            "  - {}: {}.",
            label("Missing keys"),
            render_list(&missing)
        );
    }
    if empty.is_empty() {
        info!("  - {}: None.", label("Empty values"));
    } else {
        warn!("  - {}: {}.", label("Empty values"), render_list(&empty));
    }
    Ok(())
}

const LABEL_WIDTH: usize = 17;

fn label(name: &str) -> String {
    format!("{name:.<LABEL_WIDTH$}")
}

fn render_list(items: &[&str]) -> String {
    if items.is_empty() {
        "None".to_owned()
    } else {
        items.join(", ")
    }
}

fn trimmed_is_empty(value: &[u8]) -> bool {
    match std::str::from_utf8(value) {
        Ok(text) => text.trim().is_empty(),
        Err(_) => value.iter().all(|byte| byte.is_ascii_whitespace()),
    }
}
