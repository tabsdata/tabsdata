//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeSet;

use tracing::{info, warn};

use crate::base::error::secret::SecretError;
use crate::kube::api::entity::secret::get_secret;
use crate::kube::context::KubeContext;

pub struct ReportInput<'a> {
    pub secret_name: &'a str,
    pub expected_keys: &'a [String],
}

pub enum ReportContext<'a> {
    Live(&'a KubeContext),
    ClusterDown,
}

pub async fn settlement(
    context: &ReportContext<'_>,
    input: &ReportInput<'_>,
) -> Result<(), SecretError> {
    info!("{}:", input.secret_name);
    let kube_context = match context {
        ReportContext::ClusterDown => {
            warn!(
                "  - Settlement check skipped: the cluster is stopped. \
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

    render_keys_line(false, "Extra keys", &extra);
    render_keys_line(true, "Missing keys", &missing);
    render_keys_line(true, "Empty values", &empty);
    Ok(())
}

fn render_keys_line(as_warning: bool, caption: &str, items: &[&str]) {
    if items.is_empty() {
        info!("  - {}: None.", label(caption));
        return;
    }
    if as_warning {
        warn!("  - {}:", label(caption));
        for item in items {
            warn!("    - {item}");
        }
    } else {
        info!("  - {}:", label(caption));
        for item in items {
            info!("    - {item}");
        }
    }
}

const LABEL_WIDTH: usize = 17;

fn label(name: &str) -> String {
    format!("{name:.<LABEL_WIDTH$}")
}

fn trimmed_is_empty(value: &[u8]) -> bool {
    match std::str::from_utf8(value) {
        Ok(text) => text.trim().is_empty(),
        Err(_) => value.iter().all(|byte| byte.is_ascii_whitespace()),
    }
}
