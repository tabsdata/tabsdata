//
// Copyright 2026 Tabs Data Inc.
//

use std::net::{Ipv4Addr, TcpListener};
use std::thread::sleep;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::runtime::gateway::resolver::SystemValues;

pub const DEFAULT_BASE_PORT: u16 = 2457;

const PORT_RELEASE_POLL_INTERVAL: Duration = Duration::from_millis(250);
#[cfg(not(windows))]
const PORT_RELEASE_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(windows)]
const PORT_RELEASE_TIMEOUT: Duration = Duration::from_secs(300);

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

pub fn wait_for_ports_released(assigned: &[AssignedPort]) -> Option<&AssignedPort> {
    let start = Instant::now();
    let deadline = start + PORT_RELEASE_TIMEOUT;
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let busy: Vec<u16> = assigned
            .iter()
            .filter(|entry| !port_available(entry.port))
            .map(|entry| entry.port)
            .collect();

        if busy.is_empty() {
            info!(
                attempt,
                elapsed_ms = start.elapsed().as_millis() as u64,
                "Ports release wait succeeded; all ports free"
            );
            return None;
        }

        info!(
            attempt,
            elapsed_ms = start.elapsed().as_millis() as u64,
            busy_ports = ?busy,
            "Ports release wait: ports still in use"
        );

        if Instant::now() + PORT_RELEASE_POLL_INTERVAL >= deadline {
            let entry = first_unavailable_port(assigned);
            warn!(
                attempts = attempt,
                elapsed_ms = start.elapsed().as_millis() as u64,
                busy_ports = ?busy,
                "Ports release wait timed out"
            );
            return entry;
        }
        sleep(PORT_RELEASE_POLL_INTERVAL);
    }
}

pub fn kminus_api_port(assigned: &[AssignedPort]) -> Option<u16> {
    assigned
        .iter()
        .find(|entry| entry.role == PortRole::KminusApi.as_str())
        .map(|entry| entry.port)
}

pub fn apiserver_public_port(assigned: &[AssignedPort]) -> Option<u16> {
    assigned
        .iter()
        .find(|entry| entry.role == PortRole::ApiserverPublic.as_str())
        .map(|entry| entry.port)
}

pub fn database_port(assigned: &[AssignedPort]) -> Option<u16> {
    assigned
        .iter()
        .find(|entry| entry.role == PortRole::Database.as_str())
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
