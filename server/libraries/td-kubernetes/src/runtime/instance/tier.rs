//
// Copyright 2026 Tabsdata Inc.
//

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tier {
    Init,
    Regular,
    Ephemeral,
    Authorization,
    Endpoint,
    Server,
    Setup,
}

impl Tier {
    pub fn as_str(self) -> &'static str {
        match self {
            Tier::Init => "init",
            Tier::Regular => "regular",
            Tier::Ephemeral => "ephemeral",
            Tier::Authorization => "authorization",
            Tier::Endpoint => "endpoint",
            Tier::Server => "server",
            Tier::Setup => "setup",
        }
    }

    pub fn from_label(value: &str) -> Option<Self> {
        match value {
            "init" => Some(Tier::Init),
            "regular" => Some(Tier::Regular),
            "ephemeral" => Some(Tier::Ephemeral),
            "authorization" => Some(Tier::Authorization),
            "endpoint" => Some(Tier::Endpoint),
            "server" => Some(Tier::Server),
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
