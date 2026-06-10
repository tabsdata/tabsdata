//
// Copyright 2024 Tabs Data Inc.
//

#[cfg(not(target_os = "windows"))]
use libc;
#[cfg(not(target_os = "windows"))]
use std::fs;
#[cfg(not(target_os = "windows"))]
use std::os::unix::fs::MetadataExt;

#[cfg(target_os = "windows")]
use crate::env::check_flag_env;
#[cfg(target_os = "windows")]
use crate::server::TD_DETACHED_SUBPROCESSES;
#[cfg(not(target_os = "windows"))]
use libc::pid_t;
#[cfg(not(target_os = "windows"))]
use libc::{S_IXGRP, S_IXOTH, S_IXUSR};
use std::env::consts::OS;
use std::path::{Path, PathBuf};
use std::process;
use std::time::{Duration, Instant};
use sysinfo::{Pid, Process, ProcessesToUpdate, Signal, System};
use tracing::{info, warn};

#[allow(dead_code)]
const OS_LINUX: &str = "linux";
#[allow(dead_code)]
const OS_MACOS: &str = "macos";
const OS_WINDOWS: &str = "windows";

const PROGRAM_EXTENSION_WINDOWS: &str = ".exe";
const PROGRAM_EXTENSION_UNIX: &str = "";

#[cfg(target_os = "linux")]
const EXECUTABLE_MASK: u32 = S_IXUSR | S_IXGRP | S_IXOTH;

#[cfg(target_os = "macos")]
const EXECUTABLE_MASK: u32 = (S_IXUSR | S_IXGRP | S_IXOTH) as u32;

const TERMINATE_TREE_GRACE: Duration = Duration::from_secs(32);
const TERMINATE_TREE_POLL: Duration = Duration::from_millis(100);

#[derive(Debug, thiserror::Error)]
pub enum OsProcessError {
    #[error("Process '{0}' does not exist")]
    ProcessNotFound(i32),
    #[error("Process '{0}' cannot be terminated with signal '{1}")]
    TerminationNotSupported(i32, Signal),
    #[error("Unable to terminate process '{0}' with signal '{1}")]
    TerminationFailure(i32, Signal),
}

/// Function to attach the correct extension to a program path based on platform.
pub fn name_program(path: &Path) -> PathBuf {
    let mut path = path.to_path_buf();
    let extension = match OS {
        OS_WINDOWS => PROGRAM_EXTENSION_WINDOWS,
        _ => PROGRAM_EXTENSION_UNIX,
    };
    if path.extension().is_none() {
        path.set_extension(extension.trim_start_matches('.'));
    }
    path
}

/// Function to check if a process with a given PID exists.
pub fn check_process(pid: i32) -> bool {
    let mut system = System::new_all();
    system.refresh_all();
    system.process(Pid::from_u32(pid as u32)).is_some()
}

/// Function to terminate a process with a given PID using a specified signal.
pub fn terminate_process(pid: i32, signal: Signal) -> Result<(), OsProcessError> {
    let mut system = System::new_all();
    system.refresh_all();
    if let Some(process) = system.process(Pid::from_u32(pid as u32)) {
        info!("Process with pid '{}' found: '{:?}'", pid, process.name());
        if let Some(result) = kill_with(process, signal) {
            if result {
                Ok(())
            } else {
                Err(OsProcessError::TerminationFailure(pid, signal))
            }
        } else {
            Err(OsProcessError::TerminationNotSupported(pid, signal))
        }
    } else {
        Err(OsProcessError::ProcessNotFound(pid))
    }
}

pub fn terminate_process_tree(pid: i32, signal: Signal) -> Result<(), OsProcessError> {
    let mut system = System::new_all();
    system.refresh_all();
    let Some(process) = system.process(Pid::from_u32(pid as u32)) else {
        return Err(OsProcessError::ProcessNotFound(pid));
    };
    info!("Process with pid '{}' found: '{:?}'", pid, process.name());
    kill_children_with(process, signal);
    kill_parent_with(process, signal);

    let deadline = Instant::now() + TERMINATE_TREE_GRACE;
    while Instant::now() < deadline {
        if !is_subtree_alive(pid as u32) {
            return Ok(());
        }
        std::thread::sleep(TERMINATE_TREE_POLL);
    }

    force_kill_tree(pid as u32);
    if is_subtree_alive(pid as u32) {
        Err(OsProcessError::TerminationFailure(pid, signal))
    } else {
        Ok(())
    }
}

#[cfg(not(target_os = "windows"))]
fn is_subtree_alive(root: u32) -> bool {
    if check_process(root as i32) {
        return true;
    }
    collect_descendants(root)
        .into_iter()
        .any(|pid| check_process(pid as i32))
}

#[cfg(target_os = "windows")]
fn is_subtree_alive(root: u32) -> bool {
    check_process(root as i32)
}

#[cfg(not(target_os = "windows"))]
fn force_kill_tree(root: u32) {
    let descendants = collect_descendants(root);
    let _ = process::Command::new("kill")
        .arg("-KILL")
        .arg(root.to_string())
        .output();
    for descendant in descendants {
        let _ = process::Command::new("kill")
            .arg("-KILL")
            .arg(descendant.to_string())
            .output();
    }
}

#[cfg(target_os = "windows")]
fn force_kill_tree(root: u32) {
    let mut command = process::Command::new("taskkill.exe");
    if check_flag_env(TD_DETACHED_SUBPROCESSES) {
        use std::os::windows::process::CommandExt;
        use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;
        command.creation_flags(CREATE_NO_WINDOW);
    }
    let _ = command
        .arg("/PID")
        .arg(root.to_string())
        .arg("/T")
        .arg("/F")
        .output();
}

#[cfg(not(target_os = "windows"))]
pub fn collect_descendants(root: u32) -> Vec<u32> {
    let mut system = System::new();
    system.refresh_processes(ProcessesToUpdate::All, true);
    let mut descendants: Vec<u32> = Vec::new();
    let mut frontier: Vec<Pid> = vec![Pid::from_u32(root)];
    while let Some(current) = frontier.pop() {
        for (pid, process) in system.processes() {
            if process.parent() == Some(current) {
                descendants.push(pid.as_u32());
                frontier.push(*pid);
            }
        }
    }
    descendants
}

/// Interceptor of 'sysinfo' crate function kill_with to run "taskkill /F /T ..." on Windows
/// instead of just "taskkill /F". Eventually this crate is expected to support this and this
/// function could be removed.
#[cfg(target_os = "windows")]
pub fn kill_with(process: &Process, _signal: Signal) -> Option<bool> {
    let pid = process.pid();
    let mut kill = process::Command::new("taskkill.exe");
    if check_flag_env(TD_DETACHED_SUBPROCESSES) {
        use std::os::windows::process::CommandExt;
        use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;

        kill.creation_flags(CREATE_NO_WINDOW);
    }
    kill.arg("/PID").arg(pid.to_string()).arg("/T").arg("/F");
    match kill.output() {
        Ok(output) => {
            let success = output.status.success();
            if !success {
                warn!(
                    "Failure killing process with pid '{}': exit code {:?} - error: {}",
                    pid,
                    output.status.code(),
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            } else {
                warn!("Successfully killed process with pid '{}'", pid);
            }
            Some(success)
        }
        Err(error) => {
            warn!("Error killing process with pid '{}': error: {}", pid, error);
            Some(false)
        }
    }
}

#[cfg(not(target_os = "windows"))]
pub fn kill_with(process: &Process, signal: Signal) -> Option<bool> {
    let ok_children = kill_children_with(process, signal);
    let ok_parent = kill_parent_with(process, signal);
    if !ok_children || !ok_parent {
        warn!(
            "Killing child processes or killing parent process became unnecessary: '{}' - '{}'",
            ok_children, ok_parent
        );
    }
    Some(ok_children || ok_parent)
}

#[cfg(not(target_os = "windows"))]
pub fn kill_children_with(process: &Process, signal: Signal) -> bool {
    let pid = nix::unistd::Pid::from_raw(process.pid().as_u32() as pid_t);
    let mut command = process::Command::new("pkill");
    let pkill = command
        .arg(signal_to_flag(signal))
        .arg("-P")
        .arg(pid.to_string());
    match pkill.output() {
        Ok(output) => {
            warn!(
                "Killing process tree '{}' result: '{:?}' - '{:?}'",
                pid,
                output.status,
                output.status.success()
            );
            output.status.success()
        }
        Err(error) => {
            warn!("Killing process tree '{}' raised error '{}'", pid, error);
            false
        }
    }
}

#[cfg(not(target_os = "windows"))]
pub fn kill_parent_with(process: &Process, signal: Signal) -> bool {
    let pid = nix::unistd::Pid::from_raw(process.pid().as_u32() as pid_t);
    let mut command = process::Command::new("kill");
    let kill = command.arg(signal_to_flag(signal)).arg(pid.to_string());
    match kill.output() {
        Ok(output) => {
            warn!(
                "Killing process '{}' result: '{:?}' - '{:?}'",
                pid,
                output.status,
                output.status.success()
            );
            output.status.success()
        }
        Err(error) => {
            warn!("Killing process '{}' raised error '{}'", pid, error);
            false
        }
    }
}

pub fn signal_to_flag(signal: Signal) -> String {
    match signal {
        Signal::Interrupt => "-INT",
        Signal::Term => "-TERM",
        Signal::Kill => "-KILL",
        _ => "-KILL",
    }
    .to_string()
}

#[cfg(target_os = "windows")]
pub fn is_executable(_path: &Path) -> bool {
    // Windows does not have a dedicated concept of executable file. Normally the file extension is
    // used to determine if a file is executable, but we do not want to rely on that fragile
    // assumption. Therefore, we assume all files are executable in Windows.
    true
}

#[cfg(not(target_os = "windows"))]
pub fn is_executable(path: &Path) -> bool {
    let metadata = fs::metadata(path);
    match metadata {
        Ok(metadata) => {
            let mode = metadata.mode();
            mode & EXECUTABLE_MASK != 0
        }
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::process::Command;
    use std::sync::mpsc::channel;
    use std::thread;
    use std::time::{Duration, Instant};

    use sysinfo::{ProcessesToUpdate, Signal};

    use super::*;

    #[cfg(target_os = "linux")]
    const WAIT_PROGRAM: &str = "sleep";

    #[cfg(target_os = "macos")]
    const WAIT_PROGRAM: &str = "sleep";

    #[cfg(target_os = "windows")]
    const WAIT_PROGRAM: &str = "powershell";

    // Helper function to spawn a dummy process
    fn spawn_dummy_process() -> i32 {
        let (sender, receiver) = channel();
        thread::spawn(move || {
            let mut child = if cfg!(target_os = "windows") {
                let mut command = Command::new(WAIT_PROGRAM);
                #[cfg(windows)]
                if check_flag_env(TD_DETACHED_SUBPROCESSES) {
                    use std::os::windows::process::CommandExt;
                    use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;

                    command.creation_flags(CREATE_NO_WINDOW);
                }

                command
                    .arg("-Command")
                    .arg("Start-Sleep -Seconds 600")
                    .spawn()
                    .expect("Failed to start the dummy process")
            } else {
                Command::new("sleep")
                    .arg("600")
                    .spawn()
                    .expect("Failed to start the dummy process")
            };
            sender
                .send(child.id())
                .expect("Failed to send child process back to main thread");
            child.wait().expect("Failed to wait for child process");
        });
        receiver
            .recv()
            .expect("Failed to receive child process from thread") as i32
    }

    // Test terminating a process with Linux/macOS SIGINT or Windows CTRL_C_EVENT
    #[test]
    fn test_terminate_process_with_sigint() {
        #[cfg(target_os = "linux")]
        kill_process(Signal::Interrupt);
        #[cfg(target_os = "macos")]
        kill_process(Signal::Interrupt);
        #[cfg(target_os = "windows")]
        kill_process(Signal::Kill);
    }

    // Test terminating a process with Linux/macOS SIGKILL or Windows PROCESS_TERMINATE
    #[test]
    fn test_terminate_process_with_sigkill() {
        kill_process(Signal::Kill);
    }

    // Test terminating a process with Linux/macOS SIGTERM or Windows CTRL_BREAK_EVENT
    #[test]
    fn test_terminate_process_with_sigterm() {
        #[cfg(target_os = "linux")]
        kill_process(Signal::Term);
        #[cfg(target_os = "macos")]
        kill_process(Signal::Term);
        #[cfg(target_os = "windows")]
        kill_process(Signal::Kill);
    }

    // Helper function to terminate a process with a specified signal and verify termination
    fn kill_process(signal: Signal) {
        fn is_process_alive(pid: Pid) -> bool {
            let mut system = System::new();
            system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
            system.process(pid).is_some()
        }

        let pid = spawn_dummy_process();
        let target = Pid::from_u32(pid as u32);
        assert!(
            is_process_alive(target),
            "The dummy process should be running"
        );
        terminate_process(pid, signal).expect("Failed to terminate the dummy process");

        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if !is_process_alive(target) {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!("The dummy process should be terminated after '{signal}' signal");
    }

    // Test program extension with no previous extension.
    #[test]
    #[cfg(target_os = "linux")]
    fn test_without_extension() {
        assert!(
            name_program(&PathBuf::from("program"))
                .to_string_lossy()
                .to_string()
                .eq("program")
        );
    }

    // Test program extension with no previous extension.
    #[test]
    #[cfg(target_os = "windows")]
    fn test_without_extension() {
        assert!(
            name_program(&PathBuf::from("program"))
                .to_string_lossy()
                .to_string()
                .eq("program.exe")
        );
    }

    // Test program extension with no previous extension.
    #[test]
    #[cfg(target_os = "macos")]
    fn test_without_extension() {
        assert!(
            name_program(&PathBuf::from("program"))
                .to_string_lossy()
                .to_string()
                .eq("program")
        );
    }

    // Test program extension with previous extension.
    #[test]
    #[cfg(target_os = "linux")]
    fn test_with_extension() {
        assert!(
            name_program(&PathBuf::from("program.bin"))
                .to_string_lossy()
                .to_string()
                .eq("program.bin")
        );
    }

    // Test program extension with previous extension.
    #[test]
    #[cfg(target_os = "macos")]
    fn test_with_extension() {
        assert!(
            name_program(&PathBuf::from("program.bin"))
                .to_string_lossy()
                .to_string()
                .eq("program.bin")
        );
    }

    // Test program extension with previous extension.
    #[test]
    #[cfg(target_os = "windows")]
    fn test_with_extension() {
        assert!(
            name_program(&PathBuf::from("program.bin"))
                .to_string_lossy()
                .to_string()
                .eq("program.bin")
        );
    }
}
