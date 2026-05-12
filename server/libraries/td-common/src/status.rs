//
// Copyright 2024 Tabs Data Inc.
//

#[derive(Debug)]
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

// little trick to use "?" regardless
impl From<Result<ExitStatus, ExitStatus>> for ExitStatus {
    fn from(result: Result<ExitStatus, ExitStatus>) -> Self {
        result.unwrap_or_else(|status| status)
    }
}
