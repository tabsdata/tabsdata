//
// Copyright 2024 Tabs Data Inc.
//

#[derive(Debug)]
pub enum ExitStatus {
    Success,
    NoAction,
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
            ExitStatus::NoAction => 206,
            ExitStatus::GeneralError => 201,
            ExitStatus::TabsDataError => 202,
            ExitStatus::TabsDataSigInt => 203,
            ExitStatus::TabsDataSigKill => 204,
            ExitStatus::TabsDataSigTerm => 205,
        }
    }
}
