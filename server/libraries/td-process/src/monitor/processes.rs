//
// Copyright 2025 Tabs Data Inc.
//

use std::collections::HashMap;

use crate::launcher::arg::MarkerKey::{TdAttempt, TdCollection, TdFunction, TdWorker};
use crate::monitor::memory::{physical_memory, virtual_memory};

use std::path::Path;
use sysinfo::{Pid, System};

pub type ProcessDistilled = (
    Pid,
    Pid,
    String,
    String,
    u64,
    u64,
    String,
    String,
    String,
    String,
);

const PYTHON_MODULE_ALIASES: &[(&str, &str)] = &[(
    "tabsdata.tabsserver.function.execute_function_from_bundle_path",
    "tdfunction",
)];

fn python_module_aliases() -> HashMap<&'static str, &'static str> {
    PYTHON_MODULE_ALIASES.iter().cloned().collect()
}

/// Function to get the process tree starting from a given PID.
pub fn get_process_tree(pid: i32) -> Vec<ProcessDistilled> {
    let mut system = System::new_all();
    system.refresh_all();
    process_tree(&system, Pid::from_u32(pid as u32), 0, Default::default())
}

// Helper to extract marker argument
fn extract_marker(cmd: &[String], key: &str) -> String {
    cmd.iter()
        .position(|arg| arg == key)
        .and_then(|idx| cmd.get(idx + 1))
        .map(|s| s.to_string())
        .unwrap_or_default()
}

/// Recursive function to get the process tree, with indentation and marker propagation.
fn process_tree(
    system: &System,
    pid: Pid,
    level: usize,
    parent_markers: (String, String, String, String),
) -> Vec<ProcessDistilled> {
    let process = system.process(pid);

    let base_name = process
        .map(|p| format!("{:?}", p.name()))
        .unwrap_or_else(|| String::from("<unknown>"));

    let cmd = process
        .map(|p| {
            p.cmd()
                .iter()
                .filter_map(|s| s.to_str())
                .map(String::from)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let exec = base_name.trim_matches('"');

    // In some well-known case, we try to infer a best name to be more informative:
    let mut name = if exec == "python" {
        // If running python, we try to infer the actual module being run.
        if let Some(idx) = cmd.iter().position(|arg| arg == "-m") {
            // If running a module, we extract its name, and alias it if existing in the aliases map.
            let alias_map = python_module_aliases();
            cmd.get(idx + 1)
                .map(|modname| {
                    alias_map
                        .get(modname.as_str())
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| modname.to_string())
                })
                .unwrap_or_else(|| base_name.clone())
        } else {
            // Fallback: try to infer from script/binary, only if not a flag
            cmd.get(1)
                .filter(|s| !s.starts_with('-'))
                .and_then(|s| Path::new(s).file_name())
                .and_then(|n| n.to_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| base_name.clone())
        }
    } else if exec == "bash" || exec == "sh" {
        // If running on Unixes shell, we extract the file name of the running script.
        cmd.get(1)
            .filter(|s| !s.starts_with('-'))
            .and_then(|s| Path::new(s).file_name())
            .and_then(|n| n.to_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| base_name.clone())
    } else if exec == "cmd" {
        // If running on Windows shell, we extract the file name of the running script, skipping first
        // slashed arguments
        cmd.iter()
            .skip(1)
            .find(|s| !s.starts_with('/'))
            .and_then(|s| Path::new(s).file_name())
            .and_then(|n| n.to_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| base_name.clone())
    } else {
        base_name.clone()
    };

    name = format!("{}· {}", "  ".repeat(level), name);

    let (parent_td_collection, parent_td_function, parent_td_worker, parent_td_attempt) =
        parent_markers;
    let mut collection = extract_marker(&cmd, &format!("--{}", TdCollection.as_ref()));
    let mut function = extract_marker(&cmd, &format!("--{}", TdFunction.as_ref()));
    let mut worker = extract_marker(&cmd, &format!("--{}", TdWorker.as_ref()));
    let mut attempt = extract_marker(&cmd, &format!("--{}", TdAttempt.as_ref()));

    if collection.is_empty() {
        collection = parent_td_collection.clone();
    }
    if function.is_empty() {
        function = parent_td_function.clone();
    }
    if worker.is_empty() {
        worker = parent_td_worker.clone();
    }
    if attempt.is_empty() {
        attempt = parent_td_attempt.clone();
    }

    let executable = process.map_or(String::from("<unknown>"), |p| {
        p.exe().map_or_else(
            || String::from("<undefined>"),
            |exe| exe.to_str().unwrap().to_string(),
        )
    });

    let mut subprocesses = vec![(
        pid,
        process.and_then(|p| p.parent()).unwrap_or(Pid::from(0)),
        name,
        executable,
        physical_memory(system, pid.as_u32()),
        virtual_memory(system, pid.as_u32()),
        collection.clone(),
        function.clone(),
        worker.clone(),
        attempt.clone(),
    )];

    let mut children: Vec<_> = system
        .processes()
        .iter()
        .filter_map(|(&child, child_proc)| {
            if child_proc.parent() == Some(pid) && child_proc.thread_kind().is_none() {
                Some(child)
            } else {
                None
            }
        })
        .collect();
    children.sort_by_key(|pid| pid.as_u32());

    for child in children.iter() {
        if let Some(process) = system.process(*child) {
            if process.parent() == Some(pid) && process.thread_kind().is_none() {
                subprocesses.extend(process_tree(
                    system,
                    *child,
                    level + 1,
                    (
                        collection.clone(),
                        function.clone(),
                        worker.clone(),
                        attempt.clone(),
                    ),
                ));
            }
        }
    }
    subprocesses
}
