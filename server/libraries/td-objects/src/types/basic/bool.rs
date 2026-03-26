//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::typed(bool(default = false))]
pub struct AllowDisabled;

#[ta_objects::typed(bool)]
pub struct DataChanged;

impl From<Option<HasData>> for DataChanged {
    fn from(has_data: Option<HasData>) -> Self {
        if let Some(has_data) = has_data {
            DataChanged(*has_data)
        } else {
            DataChanged(false)
        }
    }
}

#[ta_objects::typed(bool(default = true))]
#[derive(Eq, PartialEq, Hash)]
pub struct Enabled;

#[ta_objects::typed(bool(default = false))]
#[derive(Eq, PartialEq)]
pub struct Fixed;

#[ta_objects::typed(bool)]
pub struct FixedRole;

#[ta_objects::typed(bool(default = false))]
pub struct FunctionOnly;

#[ta_objects::typed(bool(default = false))]
#[derive(Eq, PartialEq, Hash)]
pub struct HasData;

#[ta_objects::typed(bool)]
#[derive(Eq, PartialEq)]
pub struct IsEmptyVersions;

#[ta_objects::typed(bool(default = false))]
pub struct OverrideDisabledTrigger;

#[ta_objects::typed(bool)]
#[derive(Eq, PartialEq)]
pub struct Partitioned;

#[ta_objects::typed(bool(default = false))]
#[derive(Eq, PartialEq)]
pub struct PasswordMustChange;

#[ta_objects::typed(bool)]
#[derive(Eq, PartialEq)]
pub struct Private;

#[ta_objects::typed(bool(default = false))]
pub struct ReuseFrozen;

#[ta_objects::typed(bool)]
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SelfDependency;

#[ta_objects::typed(bool)]
pub struct SkipIfRunning;

#[ta_objects::typed(bool)]
pub struct SysAdmin;

#[ta_objects::typed(bool)]
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct System;

#[ta_objects::typed(bool(default = false))]
pub struct UncommittedStatus;

#[ta_objects::typed(bool(default = true))]
#[derive(Eq, PartialEq)]
pub struct UserEnabled;
