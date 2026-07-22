//
// Copyright 2026 Tabs Data Inc.
//

//! Dataset sizes, selected by `TD_QUERY_LOAD_PROFILE` (`smoke` | `load` |
//! `stress`; default `smoke`). Knobs are per-family: the families scale on
//! different axes and want different magnitudes at the same profile.

use std::env;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SizeProfile {
    Smoke,
    Load,
    Stress,
}

impl SizeProfile {
    pub fn from_env() -> Self {
        match env::var("TD_QUERY_LOAD_PROFILE").as_deref() {
            Ok("load") => SizeProfile::Load,
            Ok("stress") => SizeProfile::Stress,
            _ => SizeProfile::Smoke,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            SizeProfile::Smoke => "smoke",
            SizeProfile::Load => "load",
            SizeProfile::Stress => "stress",
        }
    }

    /// `stress` is dominated by the per-row Seed-API backbone (only the run
    /// graph is bulk-inserted); run it deliberately, not in CI.
    pub fn metrics(self) -> MetricsKnobs {
        match self {
            SizeProfile::Smoke => MetricsKnobs {
                publishers: 20,
                consumers: 10,
                projects: 3,
            },
            SizeProfile::Load => MetricsKnobs {
                publishers: 1_000,
                consumers: 500,
                projects: 25,
            },
            SizeProfile::Stress => MetricsKnobs {
                publishers: 10_000,
                consumers: 5_000,
                projects: 100,
            },
        }
    }

    pub fn chain(self) -> ChainKnobs {
        match self {
            SizeProfile::Smoke => ChainKnobs { units: 10 },
            SizeProfile::Load => ChainKnobs { units: 300 },
            SizeProfile::Stress => ChainKnobs { units: 3_000 },
        }
    }
}

/// `publishers` are functions with no qualifying input (node rows + synthetic
/// edges); `consumers` each reference one publisher's output LTDV (real edges).
/// Both are spread round-robin across `projects` (min 1) so RLS scoping is
/// exercised under many tenants.
#[derive(Clone, Copy, Debug)]
pub struct MetricsKnobs {
    pub publishers: usize,
    pub consumers: usize,
    pub projects: usize,
}

/// One `unit` = a producer/consumer function pair with a dependency, an LFRI,
/// and a stamped run each.
#[derive(Clone, Copy, Debug)]
pub struct ChainKnobs {
    pub units: usize,
}
