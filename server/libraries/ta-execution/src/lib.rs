//
// Copyright 2026 Tabsdata Inc.
//

use std::fmt::Display;
use std::str::FromStr;
use td_error::{TdError, td_error};
use td_objects::dxo::graph::FunctionNode;
use td_objects::types::basic::{FunctionId, TransactionByStr, TransactionKey};
use td_objects::types::plan::PlanOptions;

/// A topology version's triggers between the functions a plan may run.
pub trait PlanGraph {
    type Trigger;

    /// The active triggers from `function`'s tables into functions the plan
    /// may run; none when `function` itself may not.
    fn triggers_from(&self, function: &FunctionId) -> Vec<&Self::Trigger>;

    fn triggered(&self, trigger: &Self::Trigger) -> FunctionId;

    /// The active triggers tying the start and the functions `triggers` reach
    /// to their children, minus the ones already in `triggers`.
    fn children_triggers<'a>(&'a self, triggers: &[&'a Self::Trigger]) -> Vec<&'a Self::Trigger>;
}

/// What an execution runs: each edition provides one.
pub trait PlanScope {
    fn plan_triggers<'a, G: PlanGraph>(
        graph: &'a G,
        start: &FunctionId,
        options: &PlanOptions,
    ) -> Vec<&'a G::Trigger>;
}

#[td_error]
pub enum TransactionMapperError {
    #[error("Missing transaction key: {0}")]
    MissingTransactionKey(TransactionKey) = 5000,
    #[error("Invalid transaction by: {0}")]
    InvalidTransactionBy(#[from] strum::ParseError) = 5001,
}

pub trait TransactionMapper:
    Default + FromStr + for<'a> TryFrom<&'a str> + Display + Sized
{
    fn key(&self, node: &FunctionNode) -> Result<TransactionKey, TdError>;

    fn transaction_by(&self) -> Result<TransactionByStr, TdError> {
        TransactionByStr::try_from(self.to_string())
    }
}
