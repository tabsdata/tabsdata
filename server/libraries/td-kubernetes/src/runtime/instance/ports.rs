//
// Copyright 2026 Tabsdata Inc.
//

use std::net::{Ipv4Addr, TcpListener};
use std::thread::sleep;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::base::error::provider::ProviderError;
use crate::client::tdkserver::widgets::show_caution_in_lines;
use crate::runtime::gateway::resolver::SystemValues;
use crate::runtime::provider::kminus::cluster::{
    BoundPort, PORT_BOUND_REASON, probe_cluster_ports,
};

pub const DEFAULT_BASE_PORT: u16 = 2457;

const PORT_REPORT_BOX_WIDTH: usize = 100;

const PORT_RELEASE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const PORT_RELEASE_LOG_INTERVAL: Duration = Duration::from_secs(5);
pub const PORT_RELEASE_TIMEOUT: Duration = Duration::from_secs(30);
pub const PORT_RELEASE_AFTER_KILL_TIMEOUT: Duration = Duration::from_secs(15);
#[cfg(windows)]
const PORT_HOLDER_KILL_GRACE: Duration = Duration::from_secs(30);

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

    fn from_str(role: &str) -> Option<Self> {
        PORT_CATALOG
            .iter()
            .copied()
            .find(|candidate| candidate.as_str() == role)
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
        && TcpListener::bind((Ipv4Addr::UNSPECIFIED, port)).is_ok()
}

pub fn ensure_ports_free(
    assigned: &[AssignedPort],
    filter: &[&str],
    patience: Duration,
) -> Result<(), ProviderError> {
    match first_port_in_use(assigned, filter, patience) {
        Some(entry) => Err(ProviderError::PortInUse {
            port: entry.port,
            caption: entry.caption.clone(),
        }),
        None => Ok(()),
    }
}

pub const CHECK_ONLY_PUBLISHED_PORTS_FILTER: &[&str] = &[PortRole::ApiserverPublic.as_str()];

pub const CHECK_ALL_PORTS_FILTER: &[&str] = &[];

pub const REPORT_ONLY_SERVED_PORTS_FILTER: &[&str] = &[
    PortRole::ApiserverPublic.as_str(),
    PortRole::ApiserverPrivate.as_str(),
    #[cfg(feature = "enterprise")]
    PortRole::AiagentPublic.as_str(),
];

pub const REPORT_ALL_PORTS_FILTER: &[&str] = &[];

pub fn ports_matching<'a>(assigned: &'a [AssignedPort], roles: &[&str]) -> Vec<&'a AssignedPort> {
    assigned
        .iter()
        .filter(|entry| roles.is_empty() || roles.contains(&entry.role.as_str()))
        .collect()
}

pub const NO_PATIENCE: Duration = Duration::ZERO;

pub fn first_port_in_use<'a>(
    assigned: &'a [AssignedPort],
    roles: &[&str],
    patience: Duration,
) -> Option<&'a AssignedPort> {
    let selected: Vec<&AssignedPort> = assigned
        .iter()
        .filter(|entry| roles.is_empty() || roles.contains(&entry.role.as_str()))
        .collect();
    if patience.is_zero() {
        return selected
            .into_iter()
            .find(|entry| !port_available(entry.port));
    }
    let owned: Vec<AssignedPort> = selected.into_iter().cloned().collect();
    let blocked = wait_for_ports_released_within(&owned, patience)?
        .role
        .clone();
    assigned.iter().find(|entry| entry.role == blocked)
}

fn first_unavailable_port(assigned: &[AssignedPort]) -> Option<&AssignedPort> {
    assigned.iter().find(|entry| !port_available(entry.port))
}

pub fn wait_for_ports_released(assigned: &[AssignedPort]) -> Option<&AssignedPort> {
    wait_for_ports_released_within(assigned, PORT_RELEASE_TIMEOUT)
}

pub fn wait_for_ports_released_within(
    assigned: &[AssignedPort],
    timeout: Duration,
) -> Option<&AssignedPort> {
    let start = Instant::now();
    let deadline = start + timeout;
    let mut last_log: Option<Instant> = None;
    loop {
        let busy: Vec<u16> = assigned
            .iter()
            .filter(|entry| !port_available(entry.port))
            .map(|entry| entry.port)
            .collect();

        if busy.is_empty() {
            info!(
                elapsed_ms = start.elapsed().as_millis() as u64,
                "Ports released"
            );
            return None;
        }

        let should_log = last_log
            .map(|at| at.elapsed() >= PORT_RELEASE_LOG_INTERVAL)
            .unwrap_or(true);
        if should_log {
            info!(
                elapsed_ms = start.elapsed().as_millis() as u64,
                busy_ports = ?busy,
                "Waiting for ports to be released"
            );
            last_log = Some(Instant::now());
        }

        if Instant::now() + PORT_RELEASE_POLL_INTERVAL >= deadline {
            warn!(
                elapsed_ms = start.elapsed().as_millis() as u64,
                busy_ports = ?busy,
                "Timed out waiting for ports to be released"
            );
            return first_unavailable_port(assigned);
        }
        sleep(PORT_RELEASE_POLL_INTERVAL);
    }
}

#[cfg(windows)]
pub fn kill_port_holders(assigned: &[AssignedPort]) {
    use crate::core::system::subprocess::terminate_process;

    let mut roots: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
    for entry in assigned {
        if port_available(entry.port) {
            continue;
        }
        let pids = listener_pids(entry.port);
        if pids.is_empty() {
            warn!(
                port = entry.port,
                caption = entry.caption.as_str(),
                "Port is bound but no holding process could be identified"
            );
            continue;
        }
        for pid in pids {
            let root = parent_pid(pid).unwrap_or(pid);
            warn!(
                port = entry.port,
                holder_pid = pid,
                root_pid = root,
                "Killing process tree holding port"
            );
            roots.insert(root);
        }
    }
    for root in roots {
        terminate_process(root, PORT_HOLDER_KILL_GRACE);
    }
}

#[cfg(not(windows))]
pub fn kill_port_holders(_assigned: &[AssignedPort]) {}

#[cfg(windows)]
fn listener_pids(port: u16) -> Vec<u32> {
    let output = std::process::Command::new("netstat")
        .args(["-ano"])
        .output();
    let Ok(output) = output else {
        return Vec::new();
    };
    let text = String::from_utf8_lossy(&output.stdout);
    let needle = format!(":{port}");
    let mut pids: Vec<u32> = Vec::new();
    for line in text.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 5 || !fields[0].eq_ignore_ascii_case("TCP") {
            continue;
        }
        if !fields[1].ends_with(&needle) {
            continue;
        }
        if !fields[3].eq_ignore_ascii_case("LISTENING") {
            continue;
        }
        if let Ok(pid) = fields[4].parse::<u32>()
            && pid != 0
            && !pids.contains(&pid)
        {
            pids.push(pid);
        }
    }
    pids
}

#[cfg(windows)]
fn parent_pid(pid: u32) -> Option<u32> {
    use sysinfo::{Pid, ProcessesToUpdate, System};
    let mut system = System::new();
    system.refresh_processes(ProcessesToUpdate::Some(&[Pid::from_u32(pid)]), true);
    system
        .process(Pid::from_u32(pid))
        .and_then(|process| process.parent())
        .map(|parent| parent.as_u32())
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

pub fn claimed_ports(assigned: &[AssignedPort]) -> Vec<BoundPort> {
    let mut claimed: Vec<BoundPort> = assigned
        .iter()
        .filter(|entry| !port_available(entry.port))
        .map(|entry| BoundPort {
            port: entry.port,
            label: entry.caption.clone(),
            reason: PORT_BOUND_REASON.to_owned(),
        })
        .collect();
    if let Some(portstart) = kminus_api_port(assigned) {
        match probe_cluster_ports(portstart) {
            Ok(bound) => {
                for entry in bound {
                    if !claimed.iter().any(|known| known.port == entry.port) {
                        claimed.push(entry);
                    }
                }
            }
            Err(error) => warn!("Could not probe the cluster port range: {error:?}."),
        }
    }
    claimed.sort_by_key(|entry| entry.port);
    claimed
}

pub fn ensure_ports_unclaimed_on_host(assigned: &[AssignedPort]) -> Result<(), ProviderError> {
    let claimed = claimed_ports(assigned);
    let Some(first) = claimed.first() else {
        return Ok(());
    };
    report_claimed_ports(&claimed);
    Err(ProviderError::PortInUse {
        port: first.port,
        caption: first.label.clone(),
    })
}

pub fn report_claimed_ports(claimed: &[BoundPort]) {
    if claimed.is_empty() {
        return;
    }
    let rendered = claimed
        .iter()
        .map(|entry| {
            format!(
                "  - {} (port {}): {}",
                entry.label, entry.port, entry.reason
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let emphasized: Vec<String> = claimed.iter().map(|entry| entry.port.to_string()).collect();
    let emphasized: Vec<&str> = emphasized.iter().map(String::as_str).collect();
    show_caution_in_lines(
        &format!(
            "Caution: {} of the port(s) this instance needs are already taken by another \
             process:\n{rendered}\nChoose a different starting port, or stop whatever holds \
             these ports, and try again.",
            claimed.len()
        ),
        PORT_REPORT_BOX_WIDTH,
        true,
        &emphasized,
    );
}

pub fn apply_ports(values: &mut SystemValues, assigned: &[AssignedPort]) {
    for entry in assigned {
        let port = entry.port.to_string();
        let Some(role) = PortRole::from_str(&entry.role) else {
            continue;
        };
        match role {
            PortRole::ApiserverPublic => values.apiserver_public_port = Some(port),
            PortRole::ApiserverPrivate => values.apiserver_private_port = Some(port),
            #[cfg(feature = "enterprise")]
            PortRole::AiagentPublic => values.aiagent_public_port = Some(port),
            PortRole::Database => values.database_port = Some(port),
            PortRole::KminusApi => values.kminus_api_port = Some(port),
        }
    }
}

#[cfg(test)]
mod tests;
