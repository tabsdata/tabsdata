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
