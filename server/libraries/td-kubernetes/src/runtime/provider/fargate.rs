//
// Copyright 2026 Tabsdata Inc.
//

use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::core::system::exit::CommandResult;
use crate::runtime::provider::ClusterProvider;
use crate::runtime::provider::cloud::CloudKubernetes;

pub async fn build(
    cluster: ClusterArguments,
    instance: InstanceArguments,
    verbose: bool,
) -> CommandResult<CloudKubernetes> {
    CloudKubernetes::build(ClusterProvider::Fargate, cluster, instance, verbose).await
}
