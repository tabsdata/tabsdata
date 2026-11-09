//
// Copyright 2026 Tabsdata Inc.
//

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PortRole {
    ApiserverPublic,
    ApiserverPrivate,
    #[cfg(feature = "enterprise")]
    AiagentPrivate,
    Database,
    KminusApi,
    #[cfg(feature = "enterprise")]
    AitdkPrivate,
    #[cfg(feature = "enterprise")]
    EmbedderPrivate,
}

impl PortRole {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ApiserverPublic => "apiserver_public_port",
            Self::ApiserverPrivate => "apiserver_private_port",
            #[cfg(feature = "enterprise")]
            Self::AiagentPrivate => "aiagent_private_port",
            Self::Database => "database_port",
            Self::KminusApi => "kminus_api_port",
            #[cfg(feature = "enterprise")]
            Self::AitdkPrivate => "aitdk_private_port",
            #[cfg(feature = "enterprise")]
            Self::EmbedderPrivate => "embedder_private_port",
        }
    }

    fn from_str(role: &str) -> Option<Self> {
        PORT_CATALOG
            .iter()
            .chain(AFTER_KMINUS)
            .copied()
            .find(|candidate| candidate.as_str() == role)
    }

    const fn caption(self) -> &'static str {
        match self {
            Self::ApiserverPublic => "apiserver public port",
            Self::ApiserverPrivate => "apiserver private port",
            #[cfg(feature = "enterprise")]
            Self::AiagentPrivate => "aiagent private port",
            Self::Database => "database port",
            Self::KminusApi => "kminus api port",
            #[cfg(feature = "enterprise")]
            Self::AitdkPrivate => "aitdk private port",
            #[cfg(feature = "enterprise")]
            Self::EmbedderPrivate => "embedder private port",
        }
    }
}

const PORT_CATALOG: &[PortRole] = &[
    PortRole::ApiserverPublic,
    PortRole::ApiserverPrivate,
    #[cfg(feature = "enterprise")]
    PortRole::AiagentPrivate,
    PortRole::Database,
    PortRole::KminusApi,
];

/// kminus binds its own components from the kminus API port on (API server,
/// controller manager, pod runner and its metrics, virtual kubelet, etcd and
/// its peer port), so later roles start after that block.
pub const KMINUS_RESERVED_PORTS: u16 = 7;

#[cfg(feature = "enterprise")]
const AFTER_KMINUS: &[PortRole] = &[PortRole::AitdkPrivate, PortRole::EmbedderPrivate];
#[cfg(not(feature = "enterprise"))]
const AFTER_KMINUS: &[PortRole] = &[];

const RENAMED_ROLES: &[(&str, PortRole)] = &[
    #[cfg(feature = "enterprise")]
    ("aiagent_public_port", PortRole::AiagentPrivate),
];

pub fn rename_roles(assigned: &mut [AssignedPort]) {
    for entry in assigned {
        if let Some((_, role)) = RENAMED_ROLES
            .iter()
            .find(|(former, _)| *former == entry.role)
        {
            entry.role = role.as_str().to_owned();
            entry.caption = role.caption().to_owned();
        }
    }
}

pub fn add_missing_roles(assigned: &mut Vec<AssignedPort>) {
    let Some(base) = assigned
        .iter()
        .find(|entry| entry.role == PortRole::ApiserverPublic.as_str())
        .map(|entry| entry.port)
    else {
        return;
    };
    for entry in assign_ports(base) {
        if !assigned.iter().any(|present| present.role == entry.role) {
            assigned.push(entry);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignedPort {
    pub role: String,
    pub caption: String,
    pub port: u16,
}

pub fn assign_ports(base: u16) -> Vec<AssignedPort> {
    let assigned = |role: &PortRole, port: u16| AssignedPort {
        role: role.as_str().to_owned(),
        caption: role.caption().to_owned(),
        port,
    };
    let after = base + PORT_CATALOG.len() as u16 - 1 + KMINUS_RESERVED_PORTS;
    PORT_CATALOG
        .iter()
        .enumerate()
        .map(|(offset, role)| assigned(role, base + offset as u16))
        .chain(
            AFTER_KMINUS
                .iter()
                .enumerate()
                .map(|(offset, role)| assigned(role, after + offset as u16)),
        )
        .collect()
}

pub fn port_available(port: u16) -> bool {
    let free = |ip: IpAddr| match TcpListener::bind(SocketAddr::new(ip, port)) {
        Ok(_) => true,
        Err(error) => ip.is_ipv6() && error.kind() != std::io::ErrorKind::AddrInUse,
    };
    free(Ipv4Addr::LOCALHOST.into())
        && free(Ipv4Addr::UNSPECIFIED.into())
        && free(Ipv6Addr::LOCALHOST.into())
        && free(Ipv6Addr::UNSPECIFIED.into())
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
    PortRole::AiagentPrivate.as_str(),
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
pub fn kill_port_holders(assigned: &[AssignedPort], owners: &[String]) {
    use crate::core::system::identity::{process_carries_owner, processes_carrying_owner};
    use crate::core::system::subprocess::force_kill_process;

    let mut held = false;
    for entry in assigned {
        if port_available(entry.port) {
            continue;
        }
        held = true;
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
            if process_carries_owner(pid, owners) {
                warn!(
                    port = entry.port,
                    holder_pid = pid,
                    "Port is held by a process left over from this instance"
                );
            } else {
                warn!(
                    port = entry.port,
                    holder_pid = pid,
                    "Port is held by a process not proven to belong to this instance; leaving it"
                );
            }
        }
    }
    if !held || owners.is_empty() {
        return;
    }
    for pid in processes_carrying_owner(owners) {
        if process_carries_owner(pid, owners) {
            warn!(pid, "Killing a process left over from this instance");
            force_kill_process(pid);
        }
    }
}

#[cfg(not(windows))]
pub fn kill_port_holders(_assigned: &[AssignedPort], _owners: &[String]) {}

#[cfg(any(windows, test))]
fn table_port(local_port: u32) -> u16 {
    u16::from_be((local_port & 0xffff) as u16)
}

#[cfg(any(windows, test))]
fn holders_of(rows: impl IntoIterator<Item = (u32, u32)>, port: u16) -> Vec<u32> {
    let mut pids: Vec<u32> = Vec::new();
    for (local_port, pid) in rows {
        if pid != 0 && table_port(local_port) == port && !pids.contains(&pid) {
            pids.push(pid);
        }
    }
    pids
}

#[cfg(windows)]
fn listener_pids(port: u16) -> Vec<u32> {
    use windows_sys::Win32::NetworkManagement::IpHelper::{
        MIB_TCP6ROW_OWNER_PID, MIB_TCP6TABLE_OWNER_PID, MIB_TCPROW_OWNER_PID,
        MIB_TCPTABLE_OWNER_PID,
    };
    use windows_sys::Win32::Networking::WinSock::{AF_INET, AF_INET6};

    let v4 = listener_table(AF_INET)
        .map(|(buffer, size)| {
            table_rows::<MIB_TCPROW_OWNER_PID>(
                &buffer,
                size,
                std::mem::offset_of!(MIB_TCPTABLE_OWNER_PID, table),
            )
            .into_iter()
            .map(|row| (row.dwLocalPort, row.dwOwningPid))
            .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let v6 = listener_table(AF_INET6)
        .map(|(buffer, size)| {
            table_rows::<MIB_TCP6ROW_OWNER_PID>(
                &buffer,
                size,
                std::mem::offset_of!(MIB_TCP6TABLE_OWNER_PID, table),
            )
            .into_iter()
            .map(|row| (row.dwLocalPort, row.dwOwningPid))
            .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    holders_of(v4.into_iter().chain(v6), port)
}

#[cfg(windows)]
fn listener_table(family: u16) -> Option<(Vec<u64>, usize)> {
    use windows_sys::Win32::Foundation::{ERROR_INSUFFICIENT_BUFFER, NO_ERROR};
    use windows_sys::Win32::NetworkManagement::IpHelper::{
        GetExtendedTcpTable, TCP_TABLE_OWNER_PID_LISTENER,
    };

    let mut buffer: Vec<u64> = Vec::new();
    let mut size = 0u32;
    for _ in 0..8 {
        let table = if buffer.is_empty() {
            std::ptr::null_mut()
        } else {
            buffer.as_mut_ptr().cast()
        };
        let status = unsafe {
            GetExtendedTcpTable(
                table,
                &mut size,
                0,
                u32::from(family),
                TCP_TABLE_OWNER_PID_LISTENER,
                0,
            )
        };
        match status {
            NO_ERROR if !buffer.is_empty() => return Some((buffer, size as usize)),
            NO_ERROR | ERROR_INSUFFICIENT_BUFFER => {
                buffer = vec![0u64; (size as usize).div_ceil(8).max(1)];
                size = (buffer.len() * 8) as u32;
            }
            _ => {
                warn!(family, status, "Cannot read the TCP listener table");
                return None;
            }
        }
    }
    None
}

#[cfg(windows)]
fn table_rows<T: Copy>(buffer: &[u64], size: usize, offset: usize) -> Vec<T> {
    let bytes = size.min(buffer.len() * 8);
    if bytes < std::mem::size_of::<u32>() {
        return Vec::new();
    }
    let base = buffer.as_ptr().cast::<u8>();
    let count = unsafe { base.cast::<u32>().read_unaligned() } as usize;
    let stride = std::mem::size_of::<T>();
    let fits = count
        .checked_mul(stride)
        .and_then(|length| length.checked_add(offset))
        .is_some_and(|end| end <= bytes);
    if !fits {
        return Vec::new();
    }
    (0..count)
        .map(|index| unsafe {
            base.add(offset + index * stride)
                .cast::<T>()
                .read_unaligned()
        })
        .collect()
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
            PortRole::AiagentPrivate => values.aiagent_private_port = Some(port),
            PortRole::Database => values.database_port = Some(port),
            PortRole::KminusApi => values.kminus_api_port = Some(port),
            #[cfg(feature = "enterprise")]
            PortRole::AitdkPrivate => values.aitdk_private_port = Some(port),
            #[cfg(feature = "enterprise")]
            PortRole::EmbedderPrivate => values.embedder_private_port = Some(port),
        }
    }
}

#[cfg(test)]
mod tests;
