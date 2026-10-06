//
// Copyright 2026 Tabsdata Inc.
//

use std::fmt::Display;
use std::future::Future;
use std::str::FromStr;
use td_error::{TdError, td_error};
use td_objects::dxo::graph::FunctionNode;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::types::basic::{
    FunctionId, FunctionOnly, OverrideDisabledTrigger, TransactionByStr, TransactionKey,
};
use td_objects::types::recursion::RecursionOptions;

/// The trigger lookups an edition composes into its execution plans.
pub trait GraphSource: Send {
    fn walk(
        &mut self,
        start: &FunctionId,
        options: &RecursionOptions,
    ) -> impl Future<Output = Result<Vec<TriggerDBWithNames>, TdError>> + Send;
}

/// Which functions an execution runs: each edition provides one.
pub trait PlanScope {
    fn execution_recursion_options(
        function_only: FunctionOnly,
        override_disabled_trigger: OverrideDisabledTrigger,
    ) -> RecursionOptions;

    fn plan_triggers(
        source: &mut impl GraphSource,
        start: &FunctionId,
        options: &RecursionOptions,
    ) -> impl Future<Output = Result<Vec<TriggerDBWithNames>, TdError>> + Send;
}

#[td_error]
pub enum TransactionMapperError {
    #[error("Missing transaction key: {0}")]
    MissingTransactionKey(TransactionKey) = 5000,
    #[error("Invalid transaction by: {0}")]
    InvalidTransactionBy(#[from] strum::ParseError) = 5001,
}

/// TransactionMapper trait is used to map a function version node to a transaction key.
/// Depending on the mapper, the key can have different forms. This will mark to which
/// transaction the function version node belongs.
pub trait TransactionMapper:
    Default + FromStr + for<'a> TryFrom<&'a str> + Display + Sized
{
    fn key(&self, node: &FunctionNode) -> Result<TransactionKey, TdError>;

    fn transaction_by(&self) -> Result<TransactionByStr, TdError> {
        TransactionByStr::try_from(self.to_string())
    }
}
