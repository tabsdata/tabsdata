//
// Copyright 2026 Tabs Data Inc.
//

use crate::client::registry::list_instances;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::render::report;
use crate::client::tdkserver::render::report::component::service::InstanceRow;
use crate::core::system::exit::CommandResult;
use crate::cwarn;
use crate::runtime::instance::endpoint::apiserver_public_url;
use crate::runtime::provider::provider_for_instance;

pub async fn command_list(globals: &GlobalArguments) -> CommandResult {
    let listings = list_instances()?;
    if listings.is_empty() {
        cwarn!("No tabsdata instances found.");
        return Ok(());
    }

    let mut rows = Vec::with_capacity(listings.len());
    for listing in &listings {
        let (alive, url) =
            match provider_for_instance(Some(listing.name.clone()), globals.verbose, None).await {
                Ok(provider) => {
                    if provider.service_alive().await {
                        let url = match provider.kube_context().await {
                            Ok(context) => apiserver_public_url(&context).await,
                            Err(_) => None,
                        };
                        (Some(true), url)
                    } else {
                        (Some(false), None)
                    }
                }
                Err(_) => (None, None),
            };
        rows.push(InstanceRow {
            name: &listing.name,
            folder: &listing.folder,
            settings: listing.settings.as_ref(),
            alive,
            url,
        });
    }

    report::component::service::instance_list(&rows)?;
    Ok(())
}
