//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::typed(i32(default = 0))]
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DependencyPos;

#[ta_objects::typed(i32(min = 0, default = 0))]
pub struct Depth;

impl Depth {
    pub const ZERO: Self = Self(0);
}

#[ta_objects::typed(i32)]
pub struct StatusCount;

#[ta_objects::typed(i32)]
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TableFunctionParamPos;

#[ta_objects::typed(i32(min = 0, default = 0))]
#[derive(Eq, PartialEq)]
pub struct InputIdx;

#[ta_objects::typed(i32(default = 0))]
#[derive(Eq, PartialEq)]
pub struct VersionPos;

impl VersionPos {
    pub const SINGLE_VERSION: Self = Self(-1);
    pub const NO_VERSIONS: Self = Self(-2);
}
