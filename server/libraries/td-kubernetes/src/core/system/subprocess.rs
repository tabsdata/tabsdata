//
// Copyright 2026 Tabsdata Inc.
//

use std::io::{self, BufRead, BufReader, Read};
use std::process::{Command as SystemCommand, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use sysinfo::{Pid, ProcessStatus, ProcessesToUpdate, System};
use tracing::{info, warn};

pub fn run(command: &mut SystemCommand, label: &str, verbose: bool) -> io::Result<Output> {
    if !verbose {
        return command.output();
    }
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn()?;
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdout_handle = stdout.map(|pipe| {
        let label = label.to_owned();
        thread::spawn(move || stream_lines(pipe, &label, "stdout"))
    });
    let stderr_handle = stderr.map(|pipe| {
        let label = label.to_owned();
        thread::spawn(move || stream_lines(pipe, &label, "stderr"))
    });
    let status = child.wait()?;
    let captured_stdout = stdout_handle
        .map(|h| h.join().unwrap_or_default())
        .unwrap_or_default();
    let captured_stderr = stderr_handle
        .map(|h| h.join().unwrap_or_default())
        .unwrap_or_default();
    Ok(Output {
        status,
        stdout: captured_stdout,
        stderr: captured_stderr,
    })
}

fn stream_lines<R: Read>(pipe: R, label: &str, stream: &str) -> Vec<u8> {
    let mut buffer = Vec::new();
    let reader = BufReader::new(pipe);
    for line in reader.lines() {
        match line {
            Ok(text) => {
                info!("  [{label} {stream}] {text}");
                buffer.extend_from_slice(text.as_bytes());
                buffer.push(b'\n');
            }
            Err(_) => break,
        }
    }
    buffer
}

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
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
}

pub fn is_alive(pid: u32) -> bool {
    let mut system = System::new();
    system.refresh_processes(ProcessesToUpdate::Some(&[Pid::from_u32(pid)]), true);
    match system.process(Pid::from_u32(pid)) {
        Some(process) => process.status() != ProcessStatus::Zombie,
        None => false,
    }
}

pub fn terminate_process(pid: u32, grace: Duration) {
    let descendants = collect_descendants(pid);
    request_graceful_shutdown(pid, &descendants);
    if wait_for_tree_exit(pid, &descendants, grace) {
        warn!(pid, "Process tree terminated gracefully");
        return;
    }
    warn!(
        pid,
        grace_secs = grace.as_secs(),
        "Process tree did not exit within grace period; force-killing"
    );
    force_kill_tree(pid, &descendants);
    if is_alive(pid) {
        warn!(pid, "Process still alive after force-kill");
    } else {
        warn!(pid, "Process tree force-killed");
    }
}

pub fn terminate_single_process(pid: u32, grace: Duration) {
    let descendants = collect_descendants(pid);
    request_single_graceful_shutdown(pid);
    if wait_for_tree_exit(pid, &descendants, grace) {
        warn!(pid, "Process tree terminated gracefully");
        return;
    }
    warn!(
        pid,
        grace_secs = grace.as_secs(),
        "Process tree did not exit within grace period; force-killing"
    );
    force_kill_tree(pid, &descendants);
    if is_alive(pid) {
        warn!(pid, "Process still alive after force-kill");
    } else {
        warn!(pid, "Process tree force-killed");
    }
}

fn collect_descendants(root: u32) -> Vec<u32> {
    let mut system = System::new();
    system.refresh_processes(ProcessesToUpdate::All, true);
    let root_pid = Pid::from_u32(root);
    let mut descendants: Vec<u32> = Vec::new();
    let mut frontier: Vec<Pid> = vec![root_pid];
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

fn request_graceful_shutdown(root: u32, descendants: &[u32]) {
    #[cfg(unix)]
    {
        for group in process_groups(root, descendants) {
            unsafe {
                libc::kill(-group, libc::SIGTERM);
            }
        }
    }
    #[cfg(target_os = "windows")]
    {
        use windows_sys::Win32::System::Console::{CTRL_BREAK_EVENT, GenerateConsoleCtrlEvent};
        let _ = descendants;
        unsafe {
            GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, root);
        }
    }
}

fn request_single_graceful_shutdown(pid: u32) {
    #[cfg(unix)]
    {
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
    }
    #[cfg(target_os = "windows")]
    {
        use windows_sys::Win32::System::Console::{CTRL_BREAK_EVENT, GenerateConsoleCtrlEvent};
        unsafe {
            GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pid);
        }
    }
}

#[cfg(unix)]
fn process_groups(root: u32, descendants: &[u32]) -> Vec<i32> {
    let mut groups: Vec<i32> = Vec::new();
    for pid in std::iter::once(&root).chain(descendants) {
        let group = unsafe { libc::getpgid(*pid as i32) };
        if group > 0 && !groups.contains(&group) {
            groups.push(group);
        }
    }
    groups
}

fn wait_for_tree_exit(root: u32, descendants: &[u32], timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let any_alive = is_alive(root) || descendants.iter().any(|pid| is_alive(*pid));
        if !any_alive {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(100));
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

fn force_kill_process(pid: u32) {
    #[cfg(unix)]
    {
        unsafe {
            libc::kill(pid as i32, libc::SIGKILL);
        }
    }
    #[cfg(target_os = "windows")]
    {
        let mut command = SystemCommand::new("taskkill.exe");
        apply_detached_flags(&mut command);
        let _ = command.arg("/PID").arg(pid.to_string()).arg("/F").status();
    }
}

fn force_kill_tree(root: u32, descendants: &[u32]) {
    #[cfg(unix)]
    {
        let mut survivors = collect_descendants(root);
        for pid in descendants {
            if !survivors.contains(pid) {
                survivors.push(*pid);
            }
        }
        unsafe {
            for group in process_groups(root, &survivors) {
                libc::kill(-group, libc::SIGKILL);
            }
            for pid in &survivors {
                libc::kill(*pid as i32, libc::SIGKILL);
            }
        }
    }
    #[cfg(target_os = "windows")]
    {
        let _ = descendants;
        let mut command = SystemCommand::new("taskkill.exe");
        apply_detached_flags(&mut command);
        let _ = command
            .arg("/PID")
            .arg(root.to_string())
            .arg("/T")
            .arg("/F")
            .status();
    }
}

#[cfg(test)]
mod tests;
