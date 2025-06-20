//
// Copyright 2025 Tabs Data Inc.
//

use dir_size::get_size_in_bytes;
use ignore::WalkBuilder;
use indexmap::IndexMap;
use std::fs;
use std::fs::Metadata;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt as UnixMetadataExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use td_common::env::{get_home_dir, TABSDATA_HOME_DIR};
use td_common::logging::LOG_EXTENSION;
use td_common::server::{
    DATABASE_FOLDER, ENVIRONMENTS_FOLDER, EPHEMERAL_FOLDER, INIT_FOLDER, LOG_FOLDER, PROC_FOLDER,
    REGULAR_FOLDER, REPOSITORY_FOLDER, STORAGE_FOLDER, WORKSPACE_FOLDER, WORK_FOLDER,
};

pub const TD_MONITOR_CHECK_FREQUENCY: &str = "TD_MONITOR_CHECK_FREQUENCY";
pub const MONITOR_CHECK_FREQUENCY: u64 = 60 * 15;

pub type SpaceStats = (PathBuf, u64, String);

pub fn instance_space(instance: &Path) -> IndexMap<String, SpaceStats> {
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
    let path = PathBuf::from(trimmed);
    Some(dunce::simplified(&path).to_path_buf())
}

fn folder_space(folder: PathBuf) -> (PathBuf, u64, String) {
    let bytes = get_size_in_bytes(folder.as_path()).unwrap_or(0);
    (folder.clone(), bytes, convert_to_human_gib(bytes, false))
}

fn filtered_folder_space(folder: PathBuf) -> (PathBuf, u64, String) {
    let space: u64 = WalkBuilder::new(folder.clone())
        .build()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|e| e == LOG_EXTENSION))
        .filter_map(|entry| fs::metadata(entry.path()).ok().map(|m| m.len()))
        .sum::<u64>();
    (folder, space, convert_to_human_gib(space, false))
}

fn folder_space_without_hardlinks(folder: PathBuf) -> (PathBuf, u64, String) {
    let space: u64 = WalkBuilder::new(folder.clone())
        .build()
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let metadata = fs::metadata(entry.path()).ok()?;
            let nlink = get_nlink(entry.path(), &metadata);
            if nlink == 1 {
                Some(metadata.len())
            } else {
                None
            }
        })
        .sum();
    (folder, space, convert_to_human_gib(space, false))
}

fn get_nlink(_path: &Path, _metadata: &Metadata) -> u64 {
    #[cfg(unix)]
    {
        _metadata.nlink()
    }

    #[cfg(windows)]
    {
        use std::ffi::OsStr;
        use std::os::windows::ffi::OsStrExt;
        use windows::core::PCWSTR;
        use windows::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE};
        use windows::Win32::Storage::FileSystem::{
            CreateFileW, GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
            FILE_FLAG_BACKUP_SEMANTICS, FILE_GENERIC_READ, FILE_SHARE_READ, FILE_SHARE_WRITE,
            OPEN_EXISTING,
        };

        let wide_path: Vec<u16> = OsStr::new(_path)
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();
        unsafe {
            let handle = match CreateFileW(
                PCWSTR(wide_path.as_ptr()),
                FILE_GENERIC_READ.0,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                None,
                OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS,
                Some(HANDLE(std::ptr::null_mut())),
            ) {
                Ok(h) => h,
                Err(_) => return 1,
            };
            if handle == INVALID_HANDLE_VALUE {
                return 1;
            }
            let mut info = BY_HANDLE_FILE_INFORMATION::default();
            let success = GetFileInformationByHandle(handle, &mut info).is_ok();
            let _ = CloseHandle(handle);
            if success {
                info.nNumberOfLinks as u64
            } else {
                1
            }
        }
    }
}

pub fn convert_to_human_gib(size_in_bytes: u64, abbreviated: bool) -> String {
    const GIBIBYTE: f64 = 1024.0 * 1024.0 * 1024.0;
    let size_in_gib = size_in_bytes as f64 / GIBIBYTE;
    let unit = if abbreviated { "G" } else { "GiB" };
    format!("{:.3} {}", size_in_gib, unit)
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
