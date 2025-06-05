//
// Copyright 2024 Tabs Data Inc.
//

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const WORKER_INF_FILE: &str = "inf";

#[derive(Serialize, Deserialize, Debug)]
pub struct Inf {
    pub name: String,
    pub config: PathBuf,
    pub work: PathBuf,
    pub queue: PathBuf,
}
