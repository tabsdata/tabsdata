//
// Copyright 2026 Tabsdata Inc.
//

use std::io::Write;

use super::*;

#[test]
fn the_stdio_log_lands_in_the_instance_log_folder() {
    let root = tempfile::tempdir().expect("a temporary instance root");
    let path = stdio_log(root.path(), "km.io", "20260826T120000").expect("a path");
    assert_eq!(
        path,
        root.path()
            .join(LOG_FOLDER)
            .join("km.io.20260826T120000.log")
    );
    assert!(root.path().join(LOG_FOLDER).is_dir());
}

#[test]
fn a_writer_appends_rather_than_truncates() {
    let root = tempfile::tempdir().expect("a temporary instance root");
    let path = stdio_log(root.path(), "km.io", "stamp").expect("a path");
    for line in ["first", "second"] {
        let mut writer = writer_for(&path).expect("a writer");
        writeln!(writer, "{line}").expect("the line is written");
    }
    let held = std::fs::read_to_string(&path).expect("the file is readable");
    assert_eq!(held, "first\nsecond\n");
}

#[test]
fn only_complete_lines_are_relayed() {
    let root = tempfile::tempdir().expect("a temporary instance root");
    let path = stdio_log(root.path(), "km.io", "stamp").expect("a path");
    {
        let mut writer = writer_for(&path).expect("a writer");
        write!(writer, "complete\npartial").expect("the content is written");
    }
    let file = File::open(&path).expect("the file opens");
    let mut reader = BufReader::new(file);
    let mut offset = 0u64;
    assert!(relay_available(&mut reader, &mut offset, "km", false).moved);
    assert_eq!(offset, "complete\n".len() as u64);
}

#[test]
fn the_startup_marker_is_reported_once_it_is_a_complete_line() {
    let root = tempfile::tempdir().expect("a temporary instance root");
    let path = stdio_log(root.path(), "km.io", "stamp").expect("a path");
    {
        let mut writer = writer_for(&path).expect("a writer");
        write!(writer, "booting\n{KM_STARTUP_MARKER_PREFIX} ready")
            .expect("the content is written");
    }
    let file = File::open(&path).expect("the file opens");
    let mut reader = BufReader::new(file);
    let mut offset = 0u64;

    let seen = relay_available(&mut reader, &mut offset, "km", false);
    assert!(seen.moved);
    assert!(!seen.completed, "a partial marker line is not the marker");

    {
        let mut writer = writer_for(&path).expect("a writer");
        writeln!(writer).expect("the line is completed");
    }
    assert!(relay_available(&mut reader, &mut offset, "km", false).completed);
}

#[test]
fn output_without_a_marker_reports_no_completion() {
    let root = tempfile::tempdir().expect("a temporary instance root");
    let path = stdio_log(root.path(), "km.io", "stamp").expect("a path");
    {
        let mut writer = writer_for(&path).expect("a writer");
        writeln!(writer, "booting").expect("the content is written");
    }
    let file = File::open(&path).expect("the file opens");
    let mut reader = BufReader::new(file);
    let mut offset = 0u64;
    let seen = relay_available(&mut reader, &mut offset, "km", false);
    assert!(seen.moved);
    assert!(!seen.completed);
}

#[test]
fn relaying_a_missing_file_is_harmless() {
    let root = tempfile::tempdir().expect("a temporary instance root");
    let absent = root.path().join("absent.log");
    let mut child = std::process::Command::new(env!("CARGO"))
        .arg("--version")
        .spawn()
        .expect("a short lived child");
    relay(&absent, "km", &mut child, false);
    let _ = child.wait();
}
