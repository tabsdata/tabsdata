//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::typed(i32(default = 0))]
pub struct DependencyPos;

#[td_type::typed(i32(min = 0, default = 0))]
pub struct Depth;

impl Depth {
    pub const ZERO: Self = Self(0);
}

#[td_type::typed(i32)]
pub struct StatusCount;

#[td_type::typed(i32)]
pub struct TableFunctionParamPos;

#[td_type::typed(i32(min = 0, default = 0))]
pub struct InputIdx;

#[td_type::typed(i32(default = 0))]
pub struct VersionPos;

impl VersionPos {
    pub const SINGLE_VERSION: Self = Self(-1);
    pub const NO_VERSIONS: Self = Self(-2);
}
