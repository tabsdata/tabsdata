//
// Copyright 2024 Tabs Data Inc.
//

use crate::env::check_flag_env;
use num_format::{Locale, ToFormattedString};
use std::process;
use sysinfo::{Pid, System};
use tracing::debug;

pub const MEMORY_CHECK_FREQUENCY: u64 = 10;

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

    pub fn monitor(&mut self) {
        self.system = System::new_all();
        self.system.refresh_all();
        debug!(
            "\n\
            - Process PID...: {}\n\
            - Process Physical Memory: {} mb\n\
            - Process Virtual Memory.: {} mb\n\
            - System Total Memory....: {} mb\n\
            - System Used Memory.....: {} mb\n\
            - System Free Memory.....: {} mb",
            process::id(),
            (self.physical_memory(process::id()) / (1024 * 1024)).to_formatted_string(&self.locale),
            (self.virtual_memory(process::id()) / (1024 * 1024)).to_formatted_string(&self.locale),
            (self.system.total_memory() / (1024 * 1024)).to_formatted_string(&self.locale),
            (self.system.used_memory() / (1024 * 1024)).to_formatted_string(&self.locale),
            (self.system.free_memory() / (1024 * 1024)).to_formatted_string(&self.locale)
        );
    }

    pub fn physical_memory(&self, pid: u32) -> u64 {
        physical_memory(&self.system, pid)
    }

    pub fn virtual_memory(&self, pid: u32) -> u64 {
        virtual_memory(&self.system, pid)
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

impl Default for MemoryMonitor {
    fn default() -> Self {
        Self::new()
    }
}

pub fn check_show_env() -> bool {
    const TD_SHOW_ENV: &str = "TD_SHOW_ENV";
    check_flag_env(TD_SHOW_ENV)
}
