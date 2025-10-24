//
// Copyright 2024 Tabs Data Inc.
//

//! Module that provides all the functionality to track workers running under the Tabsdata system.

use getset::{Getters, Setters};
use netstat2::{AddressFamilyFlags, ProtocolFlags, ProtocolSocketInfo, TcpState, get_sockets_info};
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::{File, OpenOptions, remove_file};
use std::io::{Read, Write};
use std::num::ParseIntError;
use std::path::PathBuf;
use std::{fs, io};
use thiserror::Error;

use td_common::os::check_process;
use td_common::server::WORKER_PID_FILE;

pub const DEFAULT_WORKER_FOLDER: &str = ".";
pub const DEFAULT_WORKER_PID: i32 = -1;
pub const UNKNOWN_WORKER_PID: u32 = 0;

#[derive(Debug)]
pub enum WorkerStatus {
    NoPidFile,
    EmptyPidFile,
    BrokenPidFile,
    Running { pid: i32 },
    NotRunning { pid: i32 },
    NotStarted,
}

#[derive(Debug, Error)]
pub enum TrackerError {
    #[error("Error creating the worker folder '{folder}': {cause}")]
    WorkerFolderCreationError {
        folder: PathBuf,
        #[source]
        cause: io::Error,
    },
    #[error("Unable to open(r) the worker pid file '{pid_path}': {cause}")]
    WorkerPidFileReadOpenError {
        pid_path: PathBuf,
        #[source]
        cause: io::Error,
    },
    #[error("Unable to read the worker pid file '{pid_path}': {cause}")]
    WorkerPidFileReadError {
        pid_path: PathBuf,
        #[source]
        cause: io::Error,
    },
    #[error("Unable to parse the worker pid file content '{pid_value}': {cause}")]
    WorkerPidFileParseError {
        pid_value: String,
        #[source]
        cause: ParseIntError,
    },
    #[error("Unable to open(w) the worker pid file '{pid_path}': {cause}")]
    WorkerPidFileWriteOpenError {
        pid_path: PathBuf,
        #[source]
        cause: io::Error,
    },
    #[error("Unable to write the worker pid file '{pid_path}': {cause}")]
    WorkerPidFileWriteError {
        pid_path: PathBuf,
        #[source]
        cause: io::Error,
    },
    #[error("Unable to delete the worker pid file '{pid_path}': {cause}")]
    WorkerPidFileDeleteError {
        pid_path: PathBuf,
        #[source]
        cause: io::Error,
    },
    #[error("Worker '{pid}' is not running")]
    WorkerNotRunning { pid: i32 },
}

// Default tracker.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Getters, Setters)]
#[getset(get = "pub")]
pub struct WorkerTracker {
    folder: PathBuf,
    #[getset(set = "pub")]
    pid: i32,
}

impl Default for WorkerTracker {
    fn default() -> Self {
        WorkerTracker {
            folder: PathBuf::from(DEFAULT_WORKER_FOLDER),
            pid: DEFAULT_WORKER_PID,
        }
    }
}

impl WorkerTracker {
    // Creates a new WorkerTracker based on the specified folder.
    pub fn new(folder: PathBuf) -> Self {
        WorkerTracker {
            folder,
            ..Default::default()
        }
    }

    // Ensures the worker folder exists, creating it if necessary.
    pub fn ensure_worker_folder_exists(&self) -> Result<(), TrackerError> {
        if !self.check_worker_folder_exists() {
            self.create_worker_folder()?;
        }
        Ok(())
    }

    // Checks if the worker folder exists.
    pub fn check_worker_folder_exists(&self) -> bool {
        !self.folder.exists()
    }

    // Creates the worker folder.
    fn create_worker_folder(&self) -> Result<(), TrackerError> {
        fs::create_dir_all(&self.folder).map_err(|e| TrackerError::WorkerFolderCreationError {
            folder: self.folder.clone(),
            cause: e,
        })?;
        Ok(())
    }

    // Gets the path to the worker pid file.
    pub fn get_worker_pid_path(&self) -> PathBuf {
        get_pid_path(self.folder.clone())
    }

    // Reads the worker pid file and returns the PID if it exists.
    pub fn read_worker_pid_file(&self) -> Result<Option<i32>, TrackerError> {
        let pid_path = self.get_worker_pid_path();
        read_pid_file(pid_path)
    }

    // Writes the PID to the worker pid file.
    pub fn write_worker_pid_file(&mut self, pid: i32) -> Result<&WorkerTracker, TrackerError> {
        let pid_path = self.get_worker_pid_path();
        write_pid_file(pid_path, pid)?;
        self.set_pid(pid);
        Ok(self)
    }

    // Deletes the worker pid file.
    pub fn delete_worker_pid_file(&self) -> Result<(), TrackerError> {
        let pid_path = self.get_worker_pid_path();
        delete_pid_file(pid_path)?;
        Ok(())
    }

    // Checks the status of the worker by reading the pid file and checking if the worker according to content PID is running.
    pub fn check_worker_status(&self) -> WorkerStatus {
        let pid_path = self.get_worker_pid_path();
        check_status(pid_path)
    }
}

/// Gets listening ports for a list of PIDs.
/// Returns a HashMap mapping PIDs to their listening ports.
pub fn get_listening_ports_for_pids(pids: &[u32]) -> HashMap<u32, Vec<u16>> {
    let address_family_flags = AddressFamilyFlags::IPV4 | AddressFamilyFlags::IPV6;
    let protocol_flags = ProtocolFlags::TCP | ProtocolFlags::UDP;
    let mut ports_by_pid: HashMap<u32, Vec<u16>> = HashMap::new();

    if let Ok(sockets) = get_sockets_info(address_family_flags, protocol_flags) {
        for socket in sockets {
            if let Some(&pid) = socket.associated_pids.first() {
                if pids.contains(&pid) {
                    let is_listening = match socket.protocol_socket_info {
                        ProtocolSocketInfo::Tcp(ref tcp_info) => tcp_info.state == TcpState::Listen,
                        ProtocolSocketInfo::Udp(_) => true,
                    };
                    if is_listening {
                        let port = socket.local_port();
                        ports_by_pid.entry(pid).or_insert_with(Vec::new).push(port);
                    }
                }
            }
        }
    }
    for ports in ports_by_pid.values_mut() {
        ports.sort_unstable();
        ports.dedup();
    }
    ports_by_pid
}

pub fn get_pid_path(work_path: PathBuf) -> PathBuf {
    work_path.join(WORKER_PID_FILE)
}

pub fn read_pid_file(pid_path: PathBuf) -> Result<Option<i32>, TrackerError> {
    let mut pid_file =
        File::open(&pid_path).map_err(|e| TrackerError::WorkerPidFileReadOpenError {
            pid_path: pid_path.clone(),
            cause: e,
        })?;
    let mut pid_contents = String::new();
    pid_file.read_to_string(&mut pid_contents).map_err(|e| {
        TrackerError::WorkerPidFileReadError {
            pid_path: pid_path.clone(),
            cause: e,
        }
    })?;
    if pid_contents.trim().is_empty() {
        Ok(None)
    } else {
        let pid_value = pid_contents.trim().parse::<i32>().map_err(|e| {
            TrackerError::WorkerPidFileParseError {
                pid_value: pid_contents.clone(),
                cause: e,
            }
        })?;
        if pid_value == DEFAULT_WORKER_PID {
            Ok(None)
        } else {
            Ok(Some(pid_value))
        }
    }
}

pub fn write_pid_file(pid_path: PathBuf, pid: i32) -> Result<(), TrackerError> {
    let mut pid_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&pid_path)
        .map_err(|e| TrackerError::WorkerPidFileWriteOpenError {
            pid_path: pid_path.clone(),
            cause: e,
        })?;
    write!(pid_file, "{pid}").map_err(|e| TrackerError::WorkerPidFileWriteError {
        pid_path: pid_path.clone(),
        cause: e,
    })?;
    Ok(())
}

pub fn delete_pid_file(pid_path: PathBuf) -> Result<(), TrackerError> {
    if pid_path.exists() {
        remove_file(&pid_path).map_err(|e| TrackerError::WorkerPidFileDeleteError {
            pid_path: pid_path.clone(),
            cause: e,
        })?;
    }
    Ok(())
}

pub fn check_status(pid_path: PathBuf) -> WorkerStatus {
    if pid_path.exists() {
        match read_pid_file(pid_path) {
            Ok(Some(pid)) => {
                if check_process(pid) {
                    WorkerStatus::Running { pid }
                } else {
                    WorkerStatus::NotRunning { pid }
                }
            }
            Ok(None) => WorkerStatus::NotStarted,
            Err(_) => WorkerStatus::BrokenPidFile,
        }
    } else {
        WorkerStatus::NoPidFile
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::create_dir_all;
    use std::process::Command;
    use std::sync::mpsc::channel;
    use std::time::Duration;
    use std::{fs, thread};
    use td_common::env::check_flag_env;
    use td_common::server::TD_DETACHED_SUBPROCESSES;
    use tempfile::tempdir;
    use tracing::info;

    #[cfg(target_os = "linux")]
    const WAIT_PROGRAM: &str = "sleep";

    #[cfg(target_os = "macos")]
    const WAIT_PROGRAM: &str = "sleep";

    #[cfg(target_os = "windows")]
    const WAIT_PROGRAM: &str = "powershell";

    // Test for writing the worker pid  file.
    #[test]
    fn test_write_worker_pid_file() {
        let worker_folder = tempdir().unwrap();
        let worker_path = worker_folder.path().to_path_buf();
        let mut worker = WorkerTracker::new(worker_path.clone());
        create_dir_all(worker_folder.path()).unwrap();

        let pid = 12345;
        worker.write_worker_pid_file(pid).unwrap();

        let pid_path = worker_path.join(WORKER_PID_FILE);
        let pid_contents = fs::read_to_string(pid_path).unwrap();
        assert_eq!(pid_contents.trim(), pid.to_string());
    }

    // Test for reading the worker pid file.
    #[test]
    fn test_read_worker_pid_file() {
        let worker_folder = tempdir().unwrap();
        let worker_path = worker_folder.path().to_path_buf();
        let mut worker = WorkerTracker::new(worker_path.clone());
        create_dir_all(worker_folder.path()).unwrap();

        let pid = 12345;
        worker.write_worker_pid_file(pid).unwrap();

        let read_pid = worker.read_worker_pid_file().unwrap().unwrap();
        assert_eq!(read_pid, pid);
    }

    // Test for deleting the worker pid file.
    #[test]
    fn test_delete_worker_pid_file() {
        let worker_folder = tempdir().unwrap();
        let worker_path = worker_folder.path().to_path_buf();
        let mut worker = WorkerTracker::new(worker_path.clone());
        create_dir_all(worker_folder.path()).unwrap();

        let pid = 12345;
        worker.write_worker_pid_file(pid).unwrap();

        worker.delete_worker_pid_file().unwrap();

        let pid_path = worker_path.join(WORKER_PID_FILE);
        assert!(!pid_path.exists());
    }

    // Test for checking the worker status based on the generated pid file and the actual worker status.
    #[test]
    fn test_check_worker_pid_file() {
        let worker_folder = tempdir().unwrap();
        let worker_path = worker_folder.path().to_path_buf();
        let mut worker = WorkerTracker::new(worker_path.clone());
        create_dir_all(worker_folder.path()).unwrap();

        let (sender, receiver) = channel();
        thread::spawn(move || {
            let mut child = if cfg!(target_os = "windows") {
                let mut command = Command::new(WAIT_PROGRAM);
                if check_flag_env(TD_DETACHED_SUBPROCESSES) {
                    #[cfg(windows)]
                    {
                        use std::os::windows::process::CommandExt;
                        use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;

                        command.creation_flags(CREATE_NO_WINDOW);
                    }
                }
                command
                    .arg("-Command")
                    .arg("Start-Sleep -Seconds 60")
                    .spawn()
                    .expect("Failed to start the tracker dummy process")
            } else {
                Command::new("sleep")
                    .arg("60")
                    .spawn()
                    .expect("Failed to start the tracker dummy process")
            };

            worker.write_worker_pid_file(child.id() as i32).unwrap();
            let mut attempts = 60;
            while attempts > 0 {
                if let WorkerStatus::Running { pid } = worker.check_worker_status() {
                    info!("The tracker dummy worker is running with pid {}", pid);
                    worker.delete_worker_pid_file().unwrap();
                    let _ = child.kill();
                    sender.send("ok").expect("Unable to send ok message");
                    return;
                }
                thread::sleep(Duration::from_secs(1));
                attempts -= 1;
            }
            sender.send("ko").expect("Unable to send ko message");
        });
        let check = receiver
            .recv_timeout(Duration::from_secs(10))
            .expect("Unable to receive check message");
        if check != "ok" {
            panic!("The dummy worker should be running...");
        }
    }
}
