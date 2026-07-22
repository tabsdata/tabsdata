//
// Copyright 2026 Tabsdata Inc.
//

#![allow(dead_code)]

//! Placeholder kind taxonomy. Each variant's classification is:
//!
//! ```text
//! +-----------------------+----------+--------+-----------+-----------+
//! |         kind          | editable |  auto  |   phase   | supported |
//! +-----------------------+----------+--------+-----------+-----------+
//! | literal / literalAuto | yes      | no/yes | Creation  | yes       |
//! | config / configAuto   | yes      | no/yes | Execution | no        |
//! | secret / secretAuto   | yes      | no/yes | Execution | yes       |
//! | system / systemAuto   | no       | no/yes | Creation  | yes       |
//! | runtime / runtimeAuto | no       | no/yes | Execution | no        |
//! | env                   | no       | no     | Execution | yes       |
//! | var                   | no       | no     | Creation  | no        |
//! +-----------------------+----------+--------+-----------+-----------+
//! ```

use std::str::FromStr;

use crate::runtime::gateway::resolver::walker::Phase;

/// How a placeholder's value is sourced and what it leaves in the staged file.
///
/// * `Fixed`    — the user cannot supply the value: we substitute ours (at
///   creation, e.g. `system`) or keep the token verbatim for later resolution
///   (at execution, e.g. `env`, `runtime`). The init side must keep the token
///   unchanged.
/// * `Inlined`  — the user supplies the value, kept verbatim: the whole init
///   scalar becomes the staged value (e.g. `literal`).
/// * `Retained` — the user supplies the value, but the token is kept in the
///   staged file and the value is collected elsewhere (a Secret for `secret`, a
///   ConfigMap for `config`). Cannot be embedded in surrounding text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Substitution {
    Fixed,
    Inlined,
    Retained,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaceholderKind {
    Literal,
    LiteralAuto,
    Config,
    ConfigAuto,
    Secret,
    SecretAuto,
    System,
    SystemAuto,
    Runtime,
    RuntimeAuto,
    Env,
    Var,
}

impl PlaceholderKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Literal => "literal",
            Self::LiteralAuto => "literalAuto",
            Self::Config => "config",
            Self::ConfigAuto => "configAuto",
            Self::Secret => "secret",
            Self::SecretAuto => "secretAuto",
            Self::System => "system",
            Self::SystemAuto => "systemAuto",
            Self::Runtime => "runtime",
            Self::RuntimeAuto => "runtimeAuto",
            Self::Env => "env",
            Self::Var => "var",
        }
    }

    pub const fn is_auto(self) -> bool {
        matches!(
            self,
            Self::LiteralAuto
                | Self::ConfigAuto
                | Self::SecretAuto
                | Self::SystemAuto
                | Self::RuntimeAuto
        )
    }

    pub const fn is_editable(self) -> bool {
        matches!(
            self,
            Self::Literal
                | Self::LiteralAuto
                | Self::Config
                | Self::ConfigAuto
                | Self::Secret
                | Self::SecretAuto
        )
    }

    pub const fn phase(self) -> Phase {
        match self {
            Self::Literal | Self::LiteralAuto | Self::System | Self::SystemAuto | Self::Var => {
                Phase::Creation
            }
            Self::Config
            | Self::ConfigAuto
            | Self::Secret
            | Self::SecretAuto
            | Self::Runtime
            | Self::RuntimeAuto
            | Self::Env => Phase::Execution,
        }
    }

    pub const fn behavior(self) -> Substitution {
        match self {
            Self::Literal | Self::LiteralAuto => Substitution::Inlined,
            Self::Config | Self::ConfigAuto | Self::Secret | Self::SecretAuto => {
                Substitution::Retained
            }
            Self::System
            | Self::SystemAuto
            | Self::Runtime
            | Self::RuntimeAuto
            | Self::Env
            | Self::Var => Substitution::Fixed,
        }
    }

    pub const fn requires_bare_token(self) -> bool {
        matches!(self.behavior(), Substitution::Retained)
    }

    pub const fn is_supported(self) -> bool {
        matches!(
            self,
            Self::Literal
                | Self::LiteralAuto
                | Self::Secret
                | Self::SecretAuto
                | Self::System
                | Self::SystemAuto
                | Self::Env
        )
    }

    pub fn supported() -> String {
        [
            Self::Literal,
            Self::LiteralAuto,
            Self::Secret,
            Self::SecretAuto,
            Self::System,
            Self::SystemAuto,
            Self::Env,
        ]
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>()
        .join(", ")
    }
}

impl FromStr for PlaceholderKind {
    type Err = ();

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let kind = match text {
            "literal" => Self::Literal,
            "literalAuto" => Self::LiteralAuto,
            "config" => Self::Config,
            "configAuto" => Self::ConfigAuto,
            "secret" => Self::Secret,
            "secretAuto" => Self::SecretAuto,
            "system" => Self::System,
            "systemAuto" => Self::SystemAuto,
            "runtime" => Self::Runtime,
            "runtimeAuto" => Self::RuntimeAuto,
            "env" => Self::Env,
            "var" => Self::Var,
            _ => return Err(()),
        };
        Ok(kind)
    }
}

#[cfg(test)]
mod tests;
