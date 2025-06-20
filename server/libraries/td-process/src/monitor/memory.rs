//
// Copyright 2025 Tabs Data Inc.
//

use num_format::{Locale, ToFormattedString};
use std::process;

use sysinfo::{Pid, System};

pub type MemoryStats = (String, String, String, String, String);

pub fn instance_memory(system: &System, locale: &Locale) -> MemoryStats {
    (
        physical_memory(system, process::id() / (1024 * 1024)).to_formatted_string(locale),
        (virtual_memory(system, process::id()) / (1024 * 1024)).to_formatted_string(locale),
        (system.total_memory() / (1024 * 1024)).to_formatted_string(locale),
        (system.used_memory() / (1024 * 1024)).to_formatted_string(locale),
        (system.free_memory() / (1024 * 1024)).to_formatted_string(locale),
    )
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
