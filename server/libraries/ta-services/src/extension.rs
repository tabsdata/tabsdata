//
// Copyright 2025 Tabs Data Inc.
//

pub trait ContextExt<Base, Extended> {
    fn build(base: &Base, extended: &Extended) -> Self;
}
