//
// Copyright 2024 Tabs Data Inc.
//

use crate::env::{check_flag_env, get_home_dir, TABSDATA_HOME_DIR};
use crate::logging::LOG_EXTENSION;
use crate::server::{
    DATABASE_FOLDER, ENVIRONMENTS_FOLDER, EPHEMERAL_FOLDER, INIT_FOLDER, LOG_FOLDER, PROC_FOLDER,
    REGULAR_FOLDER, REPOSITORY_FOLDER, STORAGE_FOLDER, WORKSPACE_FOLDER, WORK_FOLDER,
};
use dir_size::get_size_in_bytes;
use ignore::WalkBuilder;
use indexmap::IndexMap;
use num_format::{Locale, ToFormattedString};
use std::fmt::Write;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt as UnixMetadataExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt as WindowsMetadataExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::{fs, process};

use sysinfo::{Pid, System};
use tracing::debug;

pub const TD_MONITOR_CHECK_FREQUENCY: &str = "TD_MONITOR_CHECK_FREQUENCY";
pub const MONITOR_CHECK_FREQUENCY: u64 = 60 * 15;

pub struct MemoryMonitor {
    system: System,
    locale: Locale,
}

impl MemoryMonitor {
    pub fn new() -> Self {
        Self {
            system: System::new_all(),
            locale: Locale::en,
        }
    }

    pub fn monitor(&mut self, instance: &Option<PathBuf>) {
        self.system = System::new_all();
        self.system.refresh_all();

        let (pm, vm, tm, um, fm) = self.memory();
        let memory_log = format!(
            "\t- Process Physical Memory: {} mb\n\
             \t- Process Virtual Memory.: {} mb\n\
             \t- System Total Memory....: {} mb\n\
             \t- System Used Memory.....: {} mb\n\
             \t- System Free Memory.....: {} mb",
            pm, vm, tm, um, fm
        );

        let mut log_message = String::from("\n· Memory:\n");
        log_message.push_str(&memory_log);
        if instance.is_some() {
            let mut space_log = String::new();
            if let Some(folder) = instance {
                for (name, (path, _, human)) in self.space(folder) {
                    writeln!(&mut space_log, "\t- {}: {}", name, human).unwrap();
                    writeln!(&mut space_log, "\t\t{}", path.display()).unwrap();
                }
            }
            log_message.push_str("\n· Space:\n");
            log_message.push_str(&space_log);
        }
        debug!(
            "\n\
            · Process:\n\
            \t- PID: {}\
            {}",
            process::id(),
            log_message
        );
    }

    pub fn physical_memory(&self, pid: u32) -> u64 {
        physical_memory(&self.system, pid)
    }

    pub fn virtual_memory(&self, pid: u32) -> u64 {
        virtual_memory(&self.system, pid)
    }

    pub fn memory(&self) -> (String, String, String, String, String) {
        (
            physical_memory(&self.system, process::id() / (1024 * 1024))
                .to_formatted_string(&self.locale),
            (virtual_memory(&self.system, process::id()) / (1024 * 1024))
                .to_formatted_string(&self.locale),
            (self.system.total_memory() / (1024 * 1024)).to_formatted_string(&self.locale),
            (self.system.used_memory() / (1024 * 1024)).to_formatted_string(&self.locale),
            (self.system.free_memory() / (1024 * 1024)).to_formatted_string(&self.locale),
        )
    }

    pub fn space(&self, instance: &Path) -> IndexMap<String, (PathBuf, u64, String)> {
        instance_space(instance)
    }
}

pub fn physical_memory(system: &System, pid: u32) -> u64 {
    if pid > 0 {
        let pid = Pid::from_u32(pid);
        if let Some(process) = system.process(pid) {
            return process.memory();
        }
    }
    0
}

pub fn virtual_memory(system: &System, pid: u32) -> u64 {
    if pid > 0 {
        let pid = Pid::from_u32(pid);
        if let Some(process) = system.process(pid) {
            return process.virtual_memory();
        }
    }
    0
}

pub fn instance_space(instance: &Path) -> IndexMap<String, (PathBuf, u64, String)> {
    let mut space = IndexMap::new();

    let folder = instance;
    space.insert("Instance".to_string(), folder_space(PathBuf::from(folder)));

    let folder = instance.join(REPOSITORY_FOLDER);
    space.insert("Repository".to_string(), folder_space(folder));

    let folder = instance.join(REPOSITORY_FOLDER).join(DATABASE_FOLDER);
    space.insert("Database".to_string(), folder_space(folder));

    let folder = instance.join(REPOSITORY_FOLDER).join(STORAGE_FOLDER);
    space.insert("Storage".to_string(), folder_space(folder));

    let folder = instance.join(WORKSPACE_FOLDER);
    space.insert("Workspace".to_string(), folder_space(folder));

    let folder = instance
        .join(WORKSPACE_FOLDER)
        .join(WORK_FOLDER)
        .join(PROC_FOLDER)
        .join(INIT_FOLDER);
    space.insert("Init Workers".to_string(), folder_space(folder));

    let folder = instance
        .join(WORKSPACE_FOLDER)
        .join(WORK_FOLDER)
        .join(PROC_FOLDER)
        .join(REGULAR_FOLDER);
    space.insert("Regular Workers".to_string(), folder_space(folder));

    let folder = instance
        .join(WORKSPACE_FOLDER)
        .join(WORK_FOLDER)
        .join(PROC_FOLDER)
        .join(EPHEMERAL_FOLDER);
    space.insert("Ephemeral Workers".to_string(), folder_space(folder));

    let folder = instance
        .join(WORKSPACE_FOLDER)
        .join(WORK_FOLDER)
        .join(LOG_FOLDER);
    space.insert("Supervisor Logs".to_string(), filtered_folder_space(folder));

    let folder = instance
        .join(WORKSPACE_FOLDER)
        .join(WORK_FOLDER)
        .join(PROC_FOLDER)
        .join(INIT_FOLDER);
    space.insert(
        "Init Workers Logs".to_string(),
        filtered_folder_space(folder),
    );

    let folder = instance
        .join(WORKSPACE_FOLDER)
        .join(WORK_FOLDER)
        .join(PROC_FOLDER)
        .join(REGULAR_FOLDER);
    space.insert(
        "Regular Workers Logs".to_string(),
        filtered_folder_space(folder),
    );

    let folder = instance
        .join(WORKSPACE_FOLDER)
        .join(WORK_FOLDER)
        .join(PROC_FOLDER)
        .join(EPHEMERAL_FOLDER);
    space.insert(
        "Ephemeral Workers Logs".to_string(),
        filtered_folder_space(folder),
    );

    let folder = get_home_dir()
        .join(TABSDATA_HOME_DIR)
        .join(ENVIRONMENTS_FOLDER);
    let (folder_with_hardlinks, size_with_hardlinks, human_with_hardlinks) =
        folder_space(folder.clone());
    let (_, size_without_hardlinks, human_without_hardlinks) =
        folder_space_without_hardlinks(folder.clone());
    space.insert(
        "Python Virtual Environments".to_string(),
        (
            folder_with_hardlinks,
            size_with_hardlinks,
            human_with_hardlinks,
        ),
    );
    space.insert(
        "".to_string(),
        (
            PathBuf::from(""),
            size_without_hardlinks,
            human_without_hardlinks,
        ),
    );
    let folder = get_uv_cache_dir();
    if let Some(cache_folder) = folder {
        space.insert("UV Packages Cache".to_string(), folder_space(cache_folder));
    }

    space
}

fn get_uv_cache_dir() -> Option<PathBuf> {
    let output = Command::new("uv").arg("cache").arg("dir").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    let trimmed = stdout.trim();
    Some(PathBuf::from(trimmed))
}

fn folder_space(folder: PathBuf) -> (PathBuf, u64, String) {
    let bytes = get_size_in_bytes(folder.as_path()).unwrap_or(0);
    (folder.clone(), bytes, convert_to_human_bytes(bytes, false))
}

fn filtered_folder_space(folder: PathBuf) -> (PathBuf, u64, String) {
    let space: u64 = WalkBuilder::new(folder.clone())
        .build()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|e| e == LOG_EXTENSION))
        .filter_map(|entry| fs::metadata(entry.path()).ok().map(|m| m.len()))
        .sum::<u64>();
    (folder, space, convert_to_human_bytes(space, false))
}

fn folder_space_without_hardlinks(folder: PathBuf) -> (PathBuf, u64, String) {
    let space: u64 = WalkBuilder::new(folder.clone())
        .build()
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let metadata = fs::metadata(entry.path()).ok()?;
            let nlink = get_nlink(&metadata);
            if nlink == 1 {
                Some(metadata.len())
            } else {
                None
            }
        })
        .sum();
    (folder, space, convert_to_human_bytes(space, false))
}

fn get_nlink(metadata: &fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        metadata.nlink()
    }

    #[cfg(windows)]
    {
        metadata.number_of_links()
    }
}

pub fn convert_to_human_bytes(size_in_bytes: u64, abbreviated: bool) -> String {
    const KIBIBYTE: u64 = 1 << 10;
    const MEBIBYTE: u64 = 1 << 20;
    const GIBIBYTE: u64 = 1 << 30;
    const TEBIBYTE: u64 = 1 << 40;
    const PEBIBYTE: u64 = 1 << 50;
    const EXBIBYTE: u64 = 1 << 60;

    for ((min_bytes, max_bytes), abbr_unit, full_unit) in [
        ((1, KIBIBYTE), "B", "Bytes"),
        ((KIBIBYTE, MEBIBYTE), "K", "KiB"),
        ((MEBIBYTE, GIBIBYTE), "M", "MiB"),
        ((GIBIBYTE, TEBIBYTE), "G", "GiB"),
        ((TEBIBYTE, PEBIBYTE), "T", "TiB"),
        ((PEBIBYTE, EXBIBYTE), "P", "PiB"),
    ] {
        if size_in_bytes < max_bytes {
            let value = size_in_bytes as f64 / min_bytes as f64;
            return format!(
                "{:.3} {}",
                value,
                if abbreviated { abbr_unit } else { full_unit }
            );
        }
    }

    let value = size_in_bytes as f64 / EXBIBYTE as f64;
    format!("{:.3} {}", value, if abbreviated { "E" } else { "EiB" })
}

impl Default for MemoryMonitor {
    fn default() -> Self {
        Self::new()
    }
}

pub fn check_show_env() -> bool {
    const TD_SHOW_ENV: &str = "TD_SHOW_ENV";
    check_flag_env(TD_SHOW_ENV)
}
