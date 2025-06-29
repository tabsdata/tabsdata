//
// Copyright 2025 Tabs Data Inc.
//

use num_format::Locale;
use std::fmt::Write;

use indexmap::IndexMap;
use std::path::{Path, PathBuf};
use std::process;

use crate::monitor::memory::{instance_memory, MemoryStats};
use crate::monitor::space::{instance_space, SpaceStats};
use sysinfo::System;
use tracing::debug;

pub const TD_RESOURCES_MONITOR_CHECK_FREQUENCY: &str = "TD_RESOURCES_MONITOR_CHECK_FREQUENCY";
pub const RESOURCES_MONITOR_CHECK_FREQUENCY: u64 = 60 * 15;

pub struct ResourcesMonitor {
    system: System,
    locale: Locale,
}

impl Default for ResourcesMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourcesMonitor {
    pub fn new() -> Self {
        Self {
            system: System::new_all(),
            locale: Locale::en,
        }
    }

    pub fn monitor(&mut self, instance: &Option<PathBuf>) {
        self.system = System::new_all();
        self.system.refresh_all();

        if let Some(folder) = instance {
            let mut log_message = String::from("\n· Memory:\n");

            let (pm, vm, tm, um, fm) = self.memory();
            let memory_log = format!(
                "\t- Process Physical Memory: {pm} mb\n\
                 \t- Process Virtual Memory.: {vm} mb\n\
                 \t- System Total Memory....: {tm} mb\n\
                 \t- System Used Memory.....: {um} mb\n\
                 \t- System Free Memory.....: {fm} mb"
            );
            log_message.push_str(&memory_log);

            let mut space_log = String::new();
            for (name, (path, _, human)) in self.space(folder) {
                writeln!(&mut space_log, "\t- {name}: {human}").unwrap();
                writeln!(&mut space_log, "\t\t{}", path.display()).unwrap();
            }
            log_message.push_str("\n· Space:\n");
            log_message.push_str(&space_log);

            debug!(
                "\n\
                · Process:\n\
                \t- PID: {}\
                {}",
                process::id(),
                log_message
            );
        }
    }

    pub fn memory(&self) -> MemoryStats {
        instance_memory(&self.system, &self.locale)
    }

    pub fn space(&self, instance: &Path) -> IndexMap<String, SpaceStats> {
        instance_space(instance)
    }
}
