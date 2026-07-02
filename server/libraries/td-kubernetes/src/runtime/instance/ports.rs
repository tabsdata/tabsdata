//
// Copyright 2026 Tabs Data Inc.
//

use std::net::{Ipv4Addr, TcpListener};

use serde::{Deserialize, Serialize};

use crate::runtime::gateway::resolver::SystemValues;

pub const DEFAULT_BASE_PORT: u16 = 2457;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PortRole {
    ApiserverPublic,
    ApiserverPrivate,
    #[cfg(feature = "enterprise")]
    AiagentPublic,
    Database,
    KminusApi,
}

impl PortRole {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ApiserverPublic => "apiserver_public_port",
            Self::ApiserverPrivate => "apiserver_private_port",
            #[cfg(feature = "enterprise")]
            Self::AiagentPublic => "aiagent_public_port",
            Self::Database => "database_port",
            Self::KminusApi => "kminus_api_port",
        }
    }

    const fn caption(self) -> &'static str {
        match self {
            Self::ApiserverPublic => "apiserver public port",
            Self::ApiserverPrivate => "apiserver private port",
            #[cfg(feature = "enterprise")]
            Self::AiagentPublic => "aiagent public port",
            Self::Database => "database port",
            Self::KminusApi => "kminus api port",
        }
    }
}

const PORT_CATALOG: &[PortRole] = &[
    PortRole::ApiserverPublic,
    PortRole::ApiserverPrivate,
    #[cfg(feature = "enterprise")]
    PortRole::AiagentPublic,
    PortRole::Database,
    PortRole::KminusApi,
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignedPort {
    pub role: String,
    pub caption: String,
    pub port: u16,
}

pub fn assign_ports(base: u16) -> Vec<AssignedPort> {
    PORT_CATALOG
        .iter()
        .enumerate()
        .map(|(offset, role)| AssignedPort {
            role: role.as_str().to_owned(),
            caption: role.caption().to_owned(),
            port: base + offset as u16,
        })
        .collect()
}

pub fn port_available(port: u16) -> bool {
    TcpListener::bind((Ipv4Addr::LOCALHOST, port)).is_ok()
}

pub fn first_unavailable_port(assigned: &[AssignedPort]) -> Option<&AssignedPort> {
    assigned.iter().find(|entry| !port_available(entry.port))
}

pub fn kminus_api_port(assigned: &[AssignedPort]) -> Option<u16> {
    assigned
        .iter()
        .find(|entry| entry.role == PortRole::KminusApi.as_str())
        .map(|entry| entry.port)
}

pub fn apply_ports(values: &mut SystemValues, assigned: &[AssignedPort]) {
    for entry in assigned {
        let port = entry.port.to_string();
        match entry.role.as_str() {
            "apiserver_public_port" => values.apiserver_public_port = Some(port),
            "apiserver_private_port" => values.apiserver_private_port = Some(port),
            #[cfg(feature = "enterprise")]
            "aiagent_public_port" => values.aiagent_public_port = Some(port),
            "database_port" => values.database_port = Some(port),
            "kminus_api_port" => values.kminus_api_port = Some(port),
            _ => {}
        }
    }
}
