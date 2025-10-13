//
// Copyright 2025 Tabs Data Inc.
//

pub mod cancel;
pub mod list;
pub mod recover;
pub mod synchrotron;

use crate::transaction::services::cancel::TransactionCancelService;
use crate::transaction::services::list::TransactionListService;
use crate::transaction::services::recover::TransactionRecoverService;
use crate::transaction::services::synchrotron::SynchrotronService;
use getset::Getters;
use ta_services::factory::ServiceFactory;

#[derive(ServiceFactory, Getters)]
#[getset(get = "pub")]
pub struct TransactionServices {
    cancel: TransactionCancelService,
    list: TransactionListService,
    recover: TransactionRecoverService,
    synchrotron: SynchrotronService,
}
