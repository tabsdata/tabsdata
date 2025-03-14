//
// Copyright 2025 Tabs Data Inc.
//

#[macro_export]
#[rustfmt::skip]
macro_rules! all_the_tuples {
    ($name:ident) => {
        $name!([]);
        $name!([E1]);
        $name!([E1, E2]);
        $name!([E1, E2, E3]);
        $name!([E1, E2, E3, E4]);
        $name!([E1, E2, E3, E4, E5]);
        $name!([E1, E2, E3, E4, E5, E6]);
        $name!([E1, E2, E3, E4, E5, E6, E7]);
        $name!([E1, E2, E3, E4, E5, E6, E7, E8]);
        $name!([E1, E2, E3, E4, E5, E6, E7, E8, E9]);
        $name!([E1, E2, E3, E4, E5, E6, E7, E8, E9, E10]);
    };
}
