//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

const SERVER_BINARY: &str = "postgres";
const FAKE_SERVER_TEST: &str = "fake_server";
const FAKE_SERVER_VARIABLE: &str = "TD_TEST_FAKE_POSTMASTER";
const FAKE_SERVER_LIFETIME: Duration = Duration::from_secs(60);
const FIND_ATTEMPTS: usize = 100;
const FIND_INTERVAL: Duration = Duration::from_millis(100);

fn server_binary(installation: &Path) -> PathBuf {
    installation
        .join("bin")
        .join(format!("{SERVER_BINARY}{EXE_SUFFIX}"))
}

fn record(data_dir: &Path, pid: &str, binary: &str) {
    fs::write(
        data_dir.join(POSTMASTER_PID_FILE),
        format!(
            "{pid}\n{}\n1761385000\n20047\n\n127.0.0.1\n0 0\nready\n",
            data_dir.display()
        ),
    )
    .unwrap();
    fs::write(
        data_dir.join(POSTMASTER_OPTS_FILE),
        format!(
            "{binary} \"-D\" \"{}\" \"-F\" \"-p\" \"20047\"\n",
            data_dir.display()
        ),
    )
    .unwrap();
}

fn install_fake_server(installation: &Path) -> PathBuf {
    let binary = server_binary(installation);
    fs::create_dir_all(binary.parent().unwrap()).unwrap();
    fs::copy(std::env::current_exe().unwrap(), &binary).unwrap();
    binary
}

fn spawn_fake_server(binary: &Path, cwd: &Path) -> std::process::Child {
    let module = module_path!().split_once("::").unwrap().1;
    let mut command = SystemCommand::new(binary);
    command
        .args(["--exact", "--ignored", "--test-threads", "1"])
        .arg(format!("{module}::{FAKE_SERVER_TEST}"))
        .env(FAKE_SERVER_VARIABLE, "1")
        .current_dir(cwd)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        use windows_sys::Win32::System::Threading::{CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW};
        command.creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
    }
    command.spawn().unwrap()
}

fn wait_until_found(data_dir: &Path) -> Option<Postmaster> {
    for _ in 0..FIND_ATTEMPTS {
        if let Some(found) = running_on(data_dir) {
            return Some(found);
        }
        std::thread::sleep(FIND_INTERVAL);
    }
    None
}

#[test]
#[ignore]
fn fake_server() {
    if std::env::var_os(FAKE_SERVER_VARIABLE).is_some() {
        std::thread::sleep(FAKE_SERVER_LIFETIME);
    }
}

#[test]
fn recorded_binary_is_the_unquoted_head_of_the_command_line() {
    let installation = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let binary = server_binary(&installation.path().join("postgresql 18"));
    record(data_dir.path(), "4242", &binary.display().to_string());

    assert_eq!(recorded_binary(data_dir.path()), Some(binary));
    assert_eq!(recorded_pid(data_dir.path()), Some(4242));
}

#[test]
fn recorded_binary_keeps_forward_slashes_as_postgres_writes_them_on_windows() {
    let data_dir = tempfile::tempdir().unwrap();
    let binary = "C:/Users/td/.theseus/postgresql/18.4.0/bin/postgres.exe";
    record(data_dir.path(), "4242", binary);

    assert_eq!(
        recorded_binary(data_dir.path()),
        Some(PathBuf::from(binary))
    );
}

#[test]
fn the_bootstrap_backend_of_initdb_names_no_server() {
    let installation = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let binary = server_binary(installation.path());
    record(data_dir.path(), "-4242", &binary.display().to_string());

    assert_eq!(running_on(data_dir.path()), None);
}

#[test]
fn a_data_directory_without_records_has_no_server() {
    let data_dir = tempfile::tempdir().unwrap();

    assert_eq!(running_on(data_dir.path()), None);
}

#[test]
fn a_live_process_running_another_binary_is_not_the_server() {
    let installation = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let binary = server_binary(installation.path());
    record(
        data_dir.path(),
        &std::process::id().to_string(),
        &binary.display().to_string(),
    );

    assert_eq!(running_on(data_dir.path()), None);
}

#[test]
fn a_server_that_outlives_pg_ctl_is_killed() {
    let installation = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let binary = install_fake_server(installation.path());
    let mut server = spawn_fake_server(&binary, data_dir.path());
    record(
        data_dir.path(),
        &server.id().to_string(),
        &binary.display().to_string(),
    );
    let found = wait_until_found(data_dir.path());

    assert_eq!(found.as_ref().map(|found| found.pid), Some(server.id()));
    assert!(stop(data_dir.path(), &found.unwrap(), false));
    server.wait().unwrap();
    assert_eq!(running_on(data_dir.path()), None);
}

#[test]
fn a_server_running_elsewhere_is_left_alone() {
    let installation = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let elsewhere = tempfile::tempdir().unwrap();
    let binary = install_fake_server(installation.path());
    let mut server = spawn_fake_server(&binary, elsewhere.path());
    record(
        data_dir.path(),
        &server.id().to_string(),
        &binary.display().to_string(),
    );
    record(
        elsewhere.path(),
        &server.id().to_string(),
        &binary.display().to_string(),
    );
    let found_elsewhere = wait_until_found(elsewhere.path());

    assert_eq!(found_elsewhere.map(|found| found.pid), Some(server.id()));
    assert_eq!(running_on(data_dir.path()), None);
    assert!(subprocess::is_alive(server.id()).unwrap());
    server.kill().unwrap();
    server.wait().unwrap();
}
