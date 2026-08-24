//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[cfg(unix)]
const PYTHON_BINARY: &str = "python";

#[cfg(unix)]
const SPAWN_DESCENDANT_IN_OWN_GROUP: &str = "\
import os, signal, sys, time
marker = sys.argv[1]
pid = os.fork()
if pid == 0:
    os.setpgrp()
    def on_term(signum, frame):
        with open(marker, 'w') as handle:
            handle.write('terminated')
        os._exit(0)
    signal.signal(signal.SIGTERM, on_term)
    sys.stdout.write(str(os.getpid()) + '\\n')
    sys.stdout.flush()
    time.sleep(30)
    os._exit(1)
os.waitpid(pid, 0)
";

#[test]
fn is_alive_returns_true_for_current_process() {
    assert!(is_alive(std::process::id()));
}

#[test]
fn is_alive_returns_false_for_unlikely_pid() {
    // u32::MAX is reserved on every platform we run on;
    // no live process can have it. This avoids racing with arbitrary PID reuse.
    assert!(!is_alive(u32::MAX));
}

#[cfg(unix)]
#[test]
fn terminate_process_kills_descendant_in_own_process_group() {
    use std::os::unix::process::CommandExt;
    use std::process::{Command as SystemCommand, Stdio};

    if SystemCommand::new(PYTHON_BINARY)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_err()
    {
        eprintln!("Skipping test; the '{PYTHON_BINARY}' interpreter is not available");
        return;
    }

    let marker = std::env::temp_dir().join(format!(
        "td_terminate_process_marker_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&marker);

    let mut command = SystemCommand::new(PYTHON_BINARY);
    command
        .arg("-c")
        .arg(SPAWN_DESCENDANT_IN_OWN_GROUP)
        .arg(&marker)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    command.process_group(0);
    let mut child = command.spawn().expect("spawn root python");
    let root = child.id();

    let descendant = read_first_pid(child.stdout.take().expect("captured stdout"));

    assert!(is_alive(root), "root must be alive before terminate");
    assert!(
        is_alive(descendant),
        "descendant must be alive before terminate"
    );
    assert_ne!(
        process_group_of(root),
        process_group_of(descendant),
        "descendant must live in its own process group to exercise the fix"
    );

    terminate_process(root, Duration::from_secs(5));

    assert!(
        wait_for_death(root, Duration::from_secs(5)),
        "root left running after terminate_process"
    );
    assert!(
        wait_for_death(descendant, Duration::from_secs(5)),
        "descendant in its own process group left running after terminate_process"
    );
    assert!(
        marker.exists(),
        "descendant in its own process group never received SIGTERM \
         (group-only-on-root signalling); it was force-killed instead"
    );

    let _ = std::fs::remove_file(&marker);
    child.wait().ok();
}

#[cfg(unix)]
#[test]
fn terminate_single_process_leaves_descendant_in_own_group_alone() {
    use std::os::unix::process::CommandExt;
    use std::process::{Command as SystemCommand, Stdio};

    if SystemCommand::new(PYTHON_BINARY)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_err()
    {
        eprintln!("Skipping test; the '{PYTHON_BINARY}' interpreter is not available");
        return;
    }

    let marker = std::env::temp_dir().join(format!(
        "td_terminate_single_process_marker_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&marker);

    let mut command = SystemCommand::new(PYTHON_BINARY);
    command
        .arg("-c")
        .arg(SPAWN_DESCENDANT_IN_OWN_GROUP)
        .arg(&marker)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    command.process_group(0);
    let mut child = command.spawn().expect("spawn root python");
    let root = child.id();

    let descendant = read_first_pid(child.stdout.take().expect("captured stdout"));

    assert!(is_alive(root), "root must be alive before terminate");
    assert!(
        is_alive(descendant),
        "descendant must be alive before terminate"
    );
    assert_ne!(
        process_group_of(root),
        process_group_of(descendant),
        "descendant must live in its own process group to exercise the contract"
    );

    terminate_single_process(root, Duration::from_secs(5));

    assert!(
        wait_for_death(root, Duration::from_secs(5)),
        "root left running after terminate_single_process"
    );
    assert!(
        is_alive(descendant),
        "descendant killed; terminate_single_process must not touch descendants"
    );
    assert!(
        !marker.exists(),
        "descendant received SIGTERM; terminate_single_process must not signal descendants"
    );

    unsafe {
        libc::kill(descendant as i32, libc::SIGKILL);
    }
    let _ = std::fs::remove_file(&marker);
    child.wait().ok();
}

#[cfg(unix)]
fn read_first_pid(stdout: impl std::io::Read) -> u32 {
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    reader.read_line(&mut line).expect("read descendant pid");
    line.trim().parse::<u32>().expect("parse descendant pid")
}

#[cfg(unix)]
fn process_group_of(pid: u32) -> i32 {
    unsafe { libc::getpgid(pid as i32) }
}

#[cfg(unix)]
fn wait_for_death(pid: u32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while is_alive(pid) {
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(50));
    }
    true
}
