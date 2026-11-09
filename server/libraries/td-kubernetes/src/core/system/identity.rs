//
// Copyright 2026 Tabsdata Inc.
//

use std::ffi::OsStr;

use sysinfo::{Pid, ProcessRefreshKind, ProcessStatus, ProcessesToUpdate, RefreshKind, UpdateKind};
use td_common::os::{OsProcessError, system_with};

pub const OWNER_SEP: char = '@';

pub const KM_KILL_TOKEN_VAR: &str = "TD_KM_KILL_TOKEN";

const TOKEN_FIELD_SEP: char = '/';

const STAMP_FORMAT: &str = "%Y%m%dT%H%M%SZ";

const UNKNOWN_STAMP: &str = "00000000T000000Z";

pub fn owner_stamp(start_time_secs: u64) -> String {
    let Ok(secs) = i64::try_from(start_time_secs) else {
        return UNKNOWN_STAMP.to_owned();
    };
    match chrono::DateTime::from_timestamp(secs, 0) {
        Some(time) => time.format(STAMP_FORMAT).to_string(),
        None => UNKNOWN_STAMP.to_owned(),
    }
}

pub fn parse_owner(owner: &str) -> Option<(u32, &str)> {
    let (pid, stamp) = owner.trim().split_once(OWNER_SEP)?;
    let stamp = stamp.split('.').next().unwrap_or(stamp);
    if stamp.is_empty() || stamp == UNKNOWN_STAMP {
        return None;
    }
    Some((pid.parse().ok()?, stamp))
}

pub fn owner_key(owner: &str) -> Option<String> {
    let (pid, stamp) = parse_owner(owner)?;
    Some(format!("{pid}{OWNER_SEP}{stamp}"))
}

pub fn token_owner_key(token: &str) -> Option<String> {
    owner_key(token.split(TOKEN_FIELD_SEP).next().unwrap_or(token))
}

pub fn carried_token<'a>(
    environ: impl IntoIterator<Item = &'a OsStr>,
    var: &str,
) -> Option<&'a str> {
    environ.into_iter().find_map(|entry| {
        let (name, value) = entry.to_str()?.split_once('=')?;
        name.eq_ignore_ascii_case(var).then_some(value)
    })
}

pub fn carries_owner<'a>(environ: impl IntoIterator<Item = &'a OsStr>, owners: &[String]) -> bool {
    carried_token(environ, KM_KILL_TOKEN_VAR)
        .and_then(token_owner_key)
        .is_some_and(|owner| owners.contains(&owner))
}

pub fn running_since(pid: u32, stamp: &str) -> Result<bool, OsProcessError> {
    let mut system = system_with(RefreshKind::nothing())?;
    let pid = Pid::from_u32(pid);
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        false,
        ProcessRefreshKind::nothing().without_tasks(),
    );
    Ok(system.process(pid).is_some_and(|process| {
        process.status() != ProcessStatus::Zombie && owner_stamp(process.start_time()) == stamp
    }))
}

pub fn process_carries_owner(pid: u32, owners: &[String]) -> bool {
    let Ok(mut system) = system_with(RefreshKind::nothing()) else {
        return false;
    };
    let pid = Pid::from_u32(pid);
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        false,
        ProcessRefreshKind::nothing()
            .without_tasks()
            .with_environ(UpdateKind::Always),
    );
    system.process(pid).is_some_and(|process| {
        process.status() != ProcessStatus::Zombie
            && carries_owner(
                process.environ().iter().map(|entry| entry.as_os_str()),
                owners,
            )
    })
}

pub fn processes_carrying_owner(owners: &[String]) -> Vec<u32> {
    let Ok(mut system) = system_with(RefreshKind::nothing()) else {
        return Vec::new();
    };
    system.refresh_processes_specifics(
        ProcessesToUpdate::All,
        true,
        ProcessRefreshKind::nothing()
            .without_tasks()
            .with_environ(UpdateKind::Always),
    );
    let own = std::process::id();
    let mut found: Vec<u32> = system
        .processes()
        .iter()
        .filter(|(pid, process)| {
            pid.as_u32() != own
                && process.status() != ProcessStatus::Zombie
                && carries_owner(
                    process.environ().iter().map(|entry| entry.as_os_str()),
                    owners,
                )
        })
        .map(|(pid, _)| pid.as_u32())
        .collect();
    found.sort_unstable();
    found
}

#[cfg(test)]
pub(crate) fn km_identity_contract() -> Vec<Vec<String>> {
    let path = std::path::PathBuf::from(tm_workspace::workspace_root!())
        .join("../tabsdata-km/mods/kcommon/contract/identity.tsv");
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("km's identity contract at {}: {error}", path.display()))
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| line.split('\t').map(str::to_owned).collect())
        .collect()
}

#[cfg(test)]
mod tests;
