//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::typed(bool)]
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

#[td_type::typed(bool(default = false))]
pub struct Fixed;

#[td_type::typed(bool)]
pub struct FixedRole;

#[td_type::typed(bool(default = false))]
pub struct HasData;

#[td_type::typed(bool(default = false))]
pub struct PasswordMustChange;

#[td_type::typed(bool)]
pub struct Private;

#[td_type::typed(bool)]
pub struct ReuseFrozen;

#[td_type::typed(bool)]
pub struct SelfDependency;

#[td_type::typed(bool)]
pub struct SysAdmin;

#[td_type::typed(bool)]
pub struct System;

#[td_type::typed(bool(default = true))]
pub struct UserEnabled;

#[td_type::typed(bool)]
pub struct Versioned;
