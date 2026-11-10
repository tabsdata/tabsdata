//
// Copyright 2026 Tabsdata Inc.
//

use super::{env_block, push_arg};

fn utf16(text: &str) -> Vec<u16> {
    text.encode_utf16().collect()
}

fn line(args: &[&str]) -> String {
    let mut line = Vec::new();
    for arg in args {
        push_arg(&mut line, &utf16(arg));
    }
    String::from_utf16(&line).expect("utf-16")
}

fn parse(line: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut chars = line.chars().peekable();
    loop {
        while chars.peek().is_some_and(|c| *c == ' ' || *c == '\t') {
            chars.next();
        }
        if chars.peek().is_none() {
            return args;
        }
        let mut arg = String::new();
        let mut quoted = false;
        loop {
            let mut backslashes = 0;
            while chars.peek() == Some(&'\\') {
                chars.next();
                backslashes += 1;
            }
            match chars.peek() {
                Some('"') => {
                    arg.extend(std::iter::repeat_n('\\', backslashes / 2));
                    chars.next();
                    if backslashes % 2 == 1 {
                        arg.push('"');
                    } else {
                        quoted = !quoted;
                    }
                }
                Some(&c) if quoted || (c != ' ' && c != '\t') => {
                    arg.extend(std::iter::repeat_n('\\', backslashes));
                    arg.push(c);
                    chars.next();
                }
                _ => {
                    arg.extend(std::iter::repeat_n('\\', backslashes));
                    break;
                }
            }
        }
        args.push(arg);
    }
}

#[test]
fn plain_arguments_are_left_unquoted() {
    assert_eq!(
        line(&["km.exe", "start", "--workdir", "C:\\tabsdata\\cluster"]),
        "km.exe start --workdir C:\\tabsdata\\cluster"
    );
}

#[test]
fn every_argument_reads_back_unchanged() {
    let args = [
        r"C:\Program Files\tabsdata\km.exe",
        "",
        "two words",
        r#"say "hi""#,
        r"trailing\",
        r"trailing space\ ",
        r#"\\server\share\"quoted""#,
        "tab\there",
        r"C:\dir\",
    ];
    assert_eq!(parse(&line(&args)), args);
}

fn block(text: &[(&str, &str)], changes: &[(&str, Option<&str>)]) -> Vec<String> {
    let units = env_block(
        text.iter().map(|(n, v)| (utf16(n), utf16(v))),
        changes.iter().map(|(n, v)| (utf16(n), v.map(utf16))),
    );
    assert_eq!(&units[units.len() - 2..], &[0, 0]);
    String::from_utf16(&units[..units.len() - 2])
        .expect("utf-16")
        .split('\0')
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect()
}

#[test]
fn changes_apply_by_name_without_regard_to_case() {
    let entries = block(
        &[("Path", r"C:\bin"), ("TEMP", r"C:\tmp"), ("Keep", "1")],
        &[
            ("PATH", Some(r"C:\other")),
            ("temp", None),
            ("KM_TOKEN", Some("x")),
        ],
    );
    assert_eq!(entries, [r"Keep=1", "KM_TOKEN=x", r"PATH=C:\other"]);
}

#[test]
fn an_empty_environment_is_still_a_valid_block() {
    assert_eq!(
        env_block(Vec::<(Vec<u16>, Vec<u16>)>::new(), Vec::new()),
        [0, 0]
    );
}

#[cfg(windows)]
#[test]
fn a_child_in_its_own_console_runs_and_reports_its_exit_code() {
    let mut command = std::process::Command::new("cmd");
    command.args(["/d", "/c", "exit 7"]);
    let mut child = super::OwnConsoleChild::spawn(&command, None).expect("spawn cmd");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll cmd") {
            break status;
        }
        assert!(std::time::Instant::now() < deadline, "cmd never exited");
        std::thread::sleep(std::time::Duration::from_millis(50));
    };
    assert_eq!(status.code(), Some(7));
}

#[cfg(windows)]
mod jobs {
    use std::ffi::OsStr;
    use std::io::{Read, Write};
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::AsRawHandle;
    use std::process::{Command, Stdio};
    use std::{mem, ptr};

    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, IsProcessInJob, JOB_OBJECT_LIMIT_BREAKAWAY_OK,
        JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JobObjectExtendedLimitInformation, OpenJobObjectW, QueryInformationJobObject,
        SetInformationJobObject,
    };
    use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION};

    use super::super::OwnConsoleChild;

    const HELPER_JOB_VAR: &str = "TD_CONSOLE_JOB_HELPER";
    const HELPER_TEST: &str = "core::system::console::tests::jobs::job_helper";
    const IN_JOB: &str = "child-in-job=";
    const JOB_OBJECT_QUERY: u32 = 4;

    struct Handle(HANDLE);

    impl Drop for Handle {
        fn drop(&mut self) {
            unsafe {
                CloseHandle(self.0);
            }
        }
    }

    fn wide_z(text: &str) -> Vec<u16> {
        OsStr::new(text)
            .encode_wide()
            .chain(std::iter::once(0))
            .collect()
    }

    fn create_job(name: &str, allow_breakaway: bool) -> Handle {
        let job = unsafe { CreateJobObjectW(ptr::null(), wide_z(name).as_ptr()) };
        assert!(!job.is_null(), "create job {name}");
        let job = Handle(job);
        if allow_breakaway {
            let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { mem::zeroed() };
            limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_BREAKAWAY_OK;
            let set = unsafe {
                SetInformationJobObject(
                    job.0,
                    JobObjectExtendedLimitInformation,
                    (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                    mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                )
            };
            assert_ne!(set, 0, "allow breakaway from job {name}");
        }
        job
    }

    fn own_job_forbids_breakaway() -> bool {
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { mem::zeroed() };
        let queried = unsafe {
            QueryInformationJobObject(
                ptr::null_mut(),
                JobObjectExtendedLimitInformation,
                (&mut limits as *mut JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                ptr::null_mut(),
            )
        };
        let allowed = JOB_OBJECT_LIMIT_BREAKAWAY_OK | JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK;
        queried != 0 && limits.BasicLimitInformation.LimitFlags & allowed == 0
    }

    fn child_in_job_when_spawned_inside(allow_breakaway: bool) -> bool {
        let name = format!(
            "td-console-job-{}-{}",
            std::process::id(),
            if allow_breakaway { "allow" } else { "forbid" }
        );
        let job = create_job(&name, allow_breakaway);
        let mut helper = Command::new(std::env::current_exe().expect("test binary"))
            .args(["--exact", HELPER_TEST, "--nocapture", "--test-threads=1"])
            .env(HELPER_JOB_VAR, &name)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn helper");
        let assigned = unsafe { AssignProcessToJobObject(job.0, helper.as_raw_handle() as HANDLE) };
        assert_ne!(assigned, 0, "assign helper to job {name}");
        writeln!(helper.stdin.take().expect("helper stdin")).expect("release helper");
        let mut output = Vec::new();
        helper
            .stdout
            .take()
            .expect("helper stdout")
            .read_to_end(&mut output)
            .expect("read helper");
        let reported = String::from_utf8_lossy(&output).lines().find_map(|line| {
            line.split_once(IN_JOB)
                .map(|(_, value)| value.trim() == "true")
        });
        let status = helper.wait().expect("wait helper");
        assert!(status.success(), "helper failed in job {name}: {status}");
        reported.expect("helper reported where its child ran")
    }

    #[test]
    fn a_job_that_forbids_breakaway_still_lets_the_child_start_inside_it() {
        assert!(child_in_job_when_spawned_inside(false));
    }

    #[test]
    fn a_job_that_allows_breakaway_does_not_keep_the_child() {
        if own_job_forbids_breakaway() {
            eprintln!("Skipping test; this test runs in a job that forbids breakaway");
            return;
        }
        assert!(!child_in_job_when_spawned_inside(true));
    }

    #[test]
    fn job_helper() {
        let Ok(name) = std::env::var(HELPER_JOB_VAR) else {
            return;
        };
        let mut release = String::new();
        std::io::stdin()
            .read_line(&mut release)
            .expect("wait for release");
        let mut command = Command::new("ping");
        command.args(["-n", "30", "127.0.0.1"]);
        let mut child = OwnConsoleChild::spawn(&command, None).expect("spawn child");
        let job = unsafe { OpenJobObjectW(JOB_OBJECT_QUERY, 0, wide_z(&name).as_ptr()) };
        assert!(!job.is_null(), "open job {name}");
        let job = Handle(job);
        let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, child.id()) };
        assert!(!process.is_null(), "open child {}", child.id());
        let process = Handle(process);
        let mut inside = 0;
        let checked = unsafe { IsProcessInJob(process.0, job.0, &mut inside) };
        let _ = child.kill();
        assert_ne!(checked, 0, "check the child's job");
        println!("{IN_JOB}{}", inside != 0);
    }
}
