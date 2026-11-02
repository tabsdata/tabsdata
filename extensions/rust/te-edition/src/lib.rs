//
// Copyright 2026 Tabsdata Inc.
//

//! The edition the binaries run. The open-source edition contributes nothing
//! beyond the open-source crates' own components and routes; the enterprise
//! edition swaps this crate for one that lists its crates' contributions.

use ta_edition::Edition;

pub fn edition() -> Edition {
    Edition::EMPTY
}

#[cfg(test)]
mod tests {
    use super::edition;

    #[test]
    fn the_open_source_edition_manages_the_apiserver_component_only() {
        let keys: Vec<&str> = edition().database().components().map(|c| c.key()).collect();
        assert_eq!(keys, ["apiserver"]);
        assert!(edition().seeders.is_empty() && edition().routes.is_empty());
    }
}
