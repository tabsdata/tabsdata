//
// Copyright 2024 Tabs Data Inc.
//

use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitStatus {
    Success,
    NoAction,
    Cancelled,
    Aborted,
    GeneralError,
    TabsDataError,
    TabsDataSigInt,
    TabsDataSigKill,
    TabsDataSigTerm,
}

impl ExitStatus {
    pub fn code(&self) -> i32 {
        match self {
            ExitStatus::Success => 0,
            ExitStatus::NoAction => 1,
            ExitStatus::Cancelled => 2,
            ExitStatus::Aborted => 3,
            ExitStatus::GeneralError => 201,
            ExitStatus::TabsDataError => 202,
            ExitStatus::TabsDataSigInt => 203,
            ExitStatus::TabsDataSigKill => 204,
            ExitStatus::TabsDataSigTerm => 205,
        }
    }
}

impl Display for ExitStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let label = match self {
            ExitStatus::Success => "Success",
            ExitStatus::NoAction => "No Action",
            ExitStatus::Cancelled => "Cancelled",
            ExitStatus::Aborted => "Aborted",
            ExitStatus::GeneralError => "General Error",
            ExitStatus::TabsDataError => "Tabsdata Error",
            ExitStatus::TabsDataSigInt => "Interrupted",
            ExitStatus::TabsDataSigKill => "Killed",
            ExitStatus::TabsDataSigTerm => "Terminated",
        };
        write!(f, "{label}")
    }
}

// little trick to use "?" regardless
impl From<Result<ExitStatus, ExitStatus>> for ExitStatus {
    fn from(result: Result<ExitStatus, ExitStatus>) -> Self {
        result.unwrap_or_else(|status| status)
    }
}
