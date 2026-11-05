//
// Copyright 2026 Tabsdata Inc.
//

// Cargo unifies features across one build, so a test build of the workspace
// rebuilds the binaries in `target/` with the features its test dependencies
// enable, and those change how the binaries behave.
pub const TEST_FEATURES: &[&str] = &[
    #[cfg(feature = "mock-env")]
    "mock-env",
    #[cfg(feature = "dummy")]
    "dummy",
    #[cfg(feature = "test-utils")]
    "test-utils",
];

pub fn is_test_build() -> bool {
    !TEST_FEATURES.is_empty()
}
