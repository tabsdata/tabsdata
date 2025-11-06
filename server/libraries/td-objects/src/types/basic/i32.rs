//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::typed(i32(default = 0))]
pub struct DependencyPos;

#[td_type::typed(i32)]
pub struct StatusCount;

#[td_type::typed(i32)]
pub struct TableFunctionParamPos;

#[td_type::typed(i32(min = 0, default = 0))]
pub struct InputIdx;

#[td_type::typed(i32(default = 0))]
pub struct VersionPos;
