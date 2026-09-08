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
    KM_IO_CAPTURE_POLL, KM_IO_CAPTURE_QUIET, KM_IO_CAPTURE_TIMEOUT, KM_STARTUP_MARKER_PREFIX,
    LOG_FOLDER,
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
        let seen = relay_available(&mut reader, &mut offset, label, verbose);
        if seen.moved {
            latest = Instant::now();
        }
        if seen.completed {
            debug!(
                elapsed_ms = started.elapsed().as_millis() as u64,
                "{label} reported startup complete"
            );
            return;
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

#[derive(Default)]
struct Relayed {
    moved: bool,
    completed: bool,
}

fn relay_available(
    reader: &mut BufReader<File>,
    offset: &mut u64,
    label: &str,
    verbose: bool,
) -> Relayed {
    let mut seen = Relayed::default();
    if reader.seek(SeekFrom::Start(*offset)).is_err() {
        return seen;
    }
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
                seen.moved = true;
                let text = line.trim_end();
                if text.starts_with(KM_STARTUP_MARKER_PREFIX) {
                    seen.completed = true;
                }
                if verbose {
                    info!("  [{label} stdio] {text}");
                } else {
                    debug!("  [{label} stdio] {text}");
                }
            }
            Err(_) => break,
        }
    }
    seen
}

#[cfg(test)]
mod tests;
