//
// Copyright 2024 Tabs Data Inc.
//

use itertools::Itertools;

pub fn comma_separated(values: &[String]) -> String {
    values.iter().join(",")
}
