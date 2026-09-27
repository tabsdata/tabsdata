//
// Copyright 2026 Tabsdata Inc.
//

use fslock::LockFile;
use std::time::Instant;

#[cfg(unix)]
use crate::base::constants::KMINUS_MIN_NOFILE;
use crate::base::constants::{FOLDER_RUNTIMES, RUNTIMES_LOCK_FILE};
use crate::client::registry::InstanceSettings;
use crate::client::tdkserver::render::report::helper::assess::{AssessCheck, AssessHelp};
use crate::core::system::exit::CommandResult;
use crate::runtime::instance::ports::{
    AssignedPort, DEFAULT_BASE_PORT, assign_ports, claimed_ports, ports_matching,
};
use crate::runtime::provider::ClusterProvider;

pub const CONCERN: &str = "Host resources";

pub const HOST_PORTS: &str = "Host ports";
pub const OPEN_FILES: &str = "Open files";
pub const RUNTIMES_LOCK: &str = "Runtimes lock";

const CODE_PORTS: &str = "assess::host::ports";
#[cfg(unix)]
const CODE_NOFILE: &str = "assess::host::nofile";
const CODE_LOCK: &str = "assess::host::lock";

pub fn assess_open_files(provider: ClusterProvider) -> CommandResult<AssessCheck> {
    #[cfg(unix)]
    let started = Instant::now();

    if provider != ClusterProvider::Kminus {
        return Ok(AssessCheck::waived(
            CONCERN,
            OPEN_FILES,
            format!(
                "a '{}' instance runs its pods in the cloud",
                provider.as_str()
            ),
        ));
    }

    #[cfg(unix)]
    {
        use td_common::limits::{Resource, limit};

        let nofile = match limit(Resource::NOFILE) {
            Ok(nofile) => nofile,
            Err(error) => {
                return Ok(AssessCheck::waived(
                    CONCERN,
                    OPEN_FILES,
                    format!("the limit could not be read: {error}"),
                ));
            }
        };
        if nofile.soft >= KMINUS_MIN_NOFILE {
            return Ok(AssessCheck::passed(
                CONCERN,
                OPEN_FILES,
                started.elapsed().as_secs_f64(),
            ));
        }
        Ok(AssessCheck::failed(
            CONCERN,
            OPEN_FILES,
            started.elapsed().as_secs_f64(),
            CODE_NOFILE,
            "This shell may open too few files for a kminus cluster",
        )
        .with_facts(vec![format!(
            "The soft limit is {}, and kminus needs {KMINUS_MIN_NOFILE}",
            nofile.soft
        )])
        .with_help(vec![AssessHelp::with_command(
            "Raise it in this shell, with command",
            format!("ulimit -n {KMINUS_MIN_NOFILE}"),
        )]))
    }

    #[cfg(not(unix))]
    Ok(AssessCheck::waived(
        CONCERN,
        OPEN_FILES,
        "this platform sets no limit on open files",
    ))
}

pub fn assess_runtimes_lock(
    provider: ClusterProvider,
    persisted: Option<&InstanceSettings>,
) -> CommandResult<AssessCheck> {
    let started = Instant::now();

    if provider != ClusterProvider::Kminus {
        return Ok(AssessCheck::waived(
            CONCERN,
            RUNTIMES_LOCK,
            format!(
                "a '{}' instance builds its environments in a pod",
                provider.as_str()
            ),
        ));
    }

    let Some(settings) = persisted else {
        return Ok(AssessCheck::skipped(
            CONCERN,
            RUNTIMES_LOCK,
            "the folder of the instance is not known yet",
        ));
    };

    let lock = settings
        .folder
        .join(FOLDER_RUNTIMES)
        .join(RUNTIMES_LOCK_FILE);
    if !lock.exists() {
        return Ok(AssessCheck::passed(
            CONCERN,
            RUNTIMES_LOCK,
            started.elapsed().as_secs_f64(),
        ));
    }

    let held = match LockFile::open(&lock) {
        Ok(mut file) => match file.try_lock_with_pid() {
            Ok(true) => {
                let _ = file.unlock();
                false
            }
            Ok(false) => true,
            Err(_) => false,
        },
        Err(_) => false,
    };
    if !held {
        return Ok(AssessCheck::passed(
            CONCERN,
            RUNTIMES_LOCK,
            started.elapsed().as_secs_f64(),
        ));
    }

    let holder = std::fs::read_to_string(&lock)
        .ok()
        .map(|text| text.trim().to_owned())
        .filter(|text| !text.is_empty());
    let mut facts = vec![format!("Lock file: {}", lock.display())];
    if let Some(holder) = holder {
        facts.push(format!("Held by process {holder}"));
    }

    Ok(AssessCheck::failed(
        CONCERN,
        RUNTIMES_LOCK,
        started.elapsed().as_secs_f64(),
        CODE_LOCK,
        "Another process is building the python environments of this instance",
    )
    .with_facts(facts)
    .with_help(vec![AssessHelp::plain(
        "Wait for it to finish, or remove the lock if nothing holds it any more",
    )]))
}

pub fn assess_host_ports(
    provider: ClusterProvider,
    persisted: Option<&InstanceSettings>,
) -> CommandResult<AssessCheck> {
    let started = Instant::now();

    let Some(filter) = provider.ports_to_check_on_host() else {
        return Ok(AssessCheck::waived(
            CONCERN,
            HOST_PORTS,
            format!(
                "a '{}' instance publishes no port on this workstation",
                provider.as_str()
            ),
        ));
    };

    let Some(settings) = persisted else {
        return Ok(AssessCheck::skipped(
            CONCERN,
            HOST_PORTS,
            "the ports of the instance are not known yet",
        ));
    };
    let assigned = if settings.ports.is_empty() {
        assign_ports(DEFAULT_BASE_PORT)
    } else {
        settings.ports.clone()
    };
    let watched: Vec<AssignedPort> = ports_matching(&assigned, filter)
        .into_iter()
        .cloned()
        .collect();

    let claimed = claimed_ports(&watched);
    if claimed.is_empty() {
        return Ok(AssessCheck::passed(
            CONCERN,
            HOST_PORTS,
            started.elapsed().as_secs_f64(),
        ));
    }

    let taken = claimed
        .iter()
        .map(|entry| format!("{} (port {}): {}", entry.label, entry.port, entry.reason))
        .collect();

    Ok(AssessCheck::failed(
        CONCERN,
        HOST_PORTS,
        started.elapsed().as_secs_f64(),
        CODE_PORTS,
        if claimed.len() == 1 {
            "One port this instance needs is already taken".to_owned()
        } else {
            format!(
                "{} ports this instance needs are already taken",
                claimed.len()
            )
        },
    )
    .with_facts(taken)
    .with_help(vec![
        AssessHelp::plain("Stop whatever holds them, or give the instance another base port"),
        AssessHelp::with_command(
            "Set the base port when creating, with command",
            "tdkserver create --port <PORT>",
        ),
    ]))
}
