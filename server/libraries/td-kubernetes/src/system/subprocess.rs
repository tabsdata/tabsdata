//
// Copyright 2026 Tabs Data Inc.
//

use std::process::Command as SystemCommand;
use std::time::{Duration, Instant};

use sysinfo::{Pid, System};

pub fn apply_detached_flags(command: &mut SystemCommand) {
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;
        command.creation_flags(CREATE_NO_WINDOW);
    }
    #[cfg(not(target_os = "windows"))]
    {
        let _ = command;
    }
}

pub fn apply_subprocess_flags(command: &mut SystemCommand) {
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        use windows_sys::Win32::System::Threading::{CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW};
        command.creation_flags(CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP);
    }
    #[cfg(not(target_os = "windows"))]
    {
        let _ = command;
    }
}

pub fn is_alive(pid: u32) -> bool {
    let mut system = System::new();
    system.refresh_processes(
        sysinfo::ProcessesToUpdate::Some(&[Pid::from_u32(pid)]),
        true,
    );
    system.process(Pid::from_u32(pid)).is_some()
}

pub fn terminate_process(pid: u32, grace: Duration) {
    request_graceful_shutdown(pid);
    if wait_for_exit(pid, grace) {
        return;
    }
    force_kill_tree(pid);
}

fn request_graceful_shutdown(pid: u32) {
    #[cfg(unix)]
    {
        use sysinfo::Signal;
        use td_common::os::terminate_process as os_terminate;
        let _ = os_terminate(pid as i32, Signal::Term);
    }
    #[cfg(target_os = "windows")]
    {
        use windows_sys::Win32::System::Console::{CTRL_BREAK_EVENT, GenerateConsoleCtrlEvent};
        unsafe {
            GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pid);
        }
    }
}

fn wait_for_exit(pid: u32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if !is_alive(pid) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn force_kill_tree(pid: u32) {
    #[cfg(unix)]
    {
        use sysinfo::Signal;
        use td_common::os::terminate_process as os_terminate;
        let _ = os_terminate(pid as i32, Signal::Kill);
    }
    #[cfg(target_os = "windows")]
    {
        let mut command = SystemCommand::new("taskkill.exe");
        apply_detached_flags(&mut command);
        let _ = command
            .arg("/PID")
            .arg(pid.to_string())
            .arg("/T")
            .arg("/F")
            .status();
    }
}

#[cfg(test)]
mod tests;
