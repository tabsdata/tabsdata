//
// Copyright 2026 Tabsdata Inc.
//

use crate::boot::boot;

mod boot;

const TD_K8S_DEVELOPMENT_ENV: &str = "TD_K8S_DEVELOPMENT";
const TD_K8S_DEVELOPMENT_TRUE: &str = "true";
const TD_K8S_DEVELOPMENT_CFG: &str = "td_k8s_development";

fn main() {
    boot();
    println!("cargo:rerun-if-env-changed={TD_K8S_DEVELOPMENT_ENV}");
    if std::env::var(TD_K8S_DEVELOPMENT_ENV).as_deref() == Ok(TD_K8S_DEVELOPMENT_TRUE) {
        println!("cargo:rustc-cfg={TD_K8S_DEVELOPMENT_CFG}");
    }
}
