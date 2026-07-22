//
// Copyright 2024 Tabsdata Inc.
//

use itertools::Itertools;

pub fn comma_separated(values: &[String]) -> String {
    values.iter().join(",")
}
