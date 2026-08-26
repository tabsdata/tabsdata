//
// Copyright 2026 Tabsdata Inc.
//

use std::fs::{File, OpenOptions, create_dir_all};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Child;
use std::time::Instant;

use tracing::{debug, info};

use crate::base::constants::{
    KM_IO_CAPTURE_POLL, KM_IO_CAPTURE_QUIET, KM_IO_CAPTURE_TIMEOUT, LOG_FOLDER,
};

pub fn stdio_log(instance_root: &Path, prefix: &str, stamp: &str) -> Option<PathBuf> {
    let folder = instance_root.join(LOG_FOLDER);
    create_dir_all(&folder).ok()?;
    Some(folder.join(format!("{prefix}.{stamp}.log")))
}

pub fn log_file_count(instance_root: &Path) -> usize {
    let folder = instance_root.join(LOG_FOLDER);
    let Ok(listed) = std::fs::read_dir(&folder) else {
        return 0;
    };
    listed
        .filter_map(Result::ok)
        .filter(|entry| entry.path().is_file())
        .count()
}

pub fn writer_for(path: &Path) -> Option<File> {
    OpenOptions::new().create(true).append(true).open(path).ok()
}

pub fn relay(path: &Path, label: &str, child: &mut Child, verbose: bool) {
    let Ok(file) = File::open(path) else {
        debug!(
            "Could not tail '{}' for {label}; its output stays in that file.",
            path.display()
        );
        return;
    };
    let mut reader = BufReader::new(file);
    let mut offset = 0u64;
    let started = Instant::now();
    let mut latest = Instant::now();
    loop {
        let moved = relay_available(&mut reader, &mut offset, label, verbose);
        if moved {
            latest = Instant::now();
        }
        if matches!(child.try_wait(), Ok(Some(_))) {
            relay_available(&mut reader, &mut offset, label, verbose);
            return;
        }
        if started.elapsed() >= KM_IO_CAPTURE_TIMEOUT {
            return;
        }
        if latest.elapsed() >= KM_IO_CAPTURE_QUIET {
            return;
        }
        std::thread::sleep(KM_IO_CAPTURE_POLL);
    }
}

fn relay_available(
    reader: &mut BufReader<File>,
    offset: &mut u64,
    label: &str,
    verbose: bool,
) -> bool {
    if reader.seek(SeekFrom::Start(*offset)).is_err() {
        return false;
    }
    let mut moved = false;
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(read) => {
                if !line.ends_with('\n') {
                    break;
                }
                *offset += read as u64;
                moved = true;
                let text = line.trim_end();
                if verbose {
                    info!("  [{label} stdio] {text}");
                } else {
                    debug!("  [{label} stdio] {text}");
                }
            }
            Err(_) => break,
        }
    }
    moved
}

#[cfg(test)]
mod tests;
