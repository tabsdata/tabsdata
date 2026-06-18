//
// Copyright 2026 Tabs Data Inc.
//

pub mod create;
pub mod init;
pub mod list;
pub mod pin;
pub mod quickstart;
pub mod register;
pub mod restart;
pub mod start;
pub mod status;
pub mod stop;
pub mod unpin;
pub mod unregister;

use clap_derive::Args;

#[derive(Debug, Clone, Args)]
pub struct InstanceNameArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        long_help = "Registered instance name. \
                     When omitted, the pinned default instance is used; \
                     when no instance is pinned, the 'tabsdata' instance is used."
    )]
    pub instance: Option<String>,
}
