//
// Copyright 2026 Tabs Data Inc.
//

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tier {
    Init,
    Regular,
    Ephemeral,
    Service,
    Setup,
}

impl Tier {
    pub fn as_str(self) -> &'static str {
        match self {
            Tier::Init => "init",
            Tier::Regular => "regular",
            Tier::Ephemeral => "ephemeral",
            Tier::Service => "service",
            Tier::Setup => "setup",
        }
    }

    pub fn from_label(value: &str) -> Option<Self> {
        match value {
            "init" => Some(Tier::Init),
            "regular" => Some(Tier::Regular),
            "ephemeral" => Some(Tier::Ephemeral),
            "service" => Some(Tier::Service),
            "setup" => Some(Tier::Setup),
            _ => None,
        }
    }
}

impl fmt::Display for Tier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
