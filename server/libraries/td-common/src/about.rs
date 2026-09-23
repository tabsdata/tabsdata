//
// Copyright 2025 Tabsdata Inc.
//

use chrono::{DateTime, FixedOffset};
use colored::Colorize;
use serde_yaml::Value;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
#[cfg(not(windows))]
use supports_color;
use unicode_width::UnicodeWidthStr;

type Stamp = BTreeMap<String, String>;

const TD_ABOUT_MANIFEST: &str = "TD_ABOUT_MANIFEST";
const TD_RESOURCE_BINARY: &str = "td_resource";
const BUILD_MANIFEST_RESOURCE: &str = "assets/manifest/BUILD";

const BUILD_TYPE: &str = "TD_VERGEN_BUILD_TYPE";
const VERSION: &str = "TD_VERSION";
const BUILD_TIMESTAMP: &str = "VERGEN_BUILD_TIMESTAMP";

const MANIFEST_KEYS: &[(&str, &str)] = &[
    ("X-Build-Date-UTC", "VERGEN_BUILD_DATE"),
    ("X-Build-Timestamp-UTC", BUILD_TIMESTAMP),
    ("X-Build-Timezone-Name", "VERGEN_BUILD_TIMEZONE_NAME"),
    ("X-Build-Timezone-Offset", "VERGEN_BUILD_TIMEZONE_OFFSET"),
    ("X-Rust-Semver", "VERGEN_RUSTC_SEMVER"),
    ("X-Rust-Channel", "VERGEN_RUSTC_CHANNEL"),
    ("X-Rust-Host-Triple", "VERGEN_RUSTC_HOST_TRIPLE"),
    ("X-Rust-Commit-Hash", "VERGEN_RUSTC_COMMIT_HASH"),
    ("X-Rust-Commit-Date", "VERGEN_RUSTC_COMMIT_DATE"),
    ("X-Rust-LLVM-Version", "VERGEN_RUSTC_LLVM_VERSION"),
    ("X-Cargo-Target-Triple", "VERGEN_CARGO_TARGET_TRIPLE"),
    ("X-Cargo-Features", "VERGEN_CARGO_FEATURES"),
    ("X-Cargo-Debug", "VERGEN_CARGO_DEBUG"),
    ("X-Cargo-Opt-Level", "VERGEN_CARGO_OPT_LEVEL"),
    ("X-Python-Version", "VERGEN_PYTHON_VERSION"),
    ("X-Python-Implementation", "VERGEN_PYTHON_IMPLEMENTATION"),
    ("X-Node-Version", "VERGEN_NODE_VERSION"),
    ("X-System-Hostname", "VERGEN_SYSINFO_HOST"),
    ("X-System-User", "VERGEN_SYSINFO_USER"),
    ("X-System-OS-Name", "VERGEN_SYSINFO_NAME"),
    ("X-System-OS-Version", "VERGEN_SYSINFO_OS_VERSION"),
    ("X-System-CPU-Vendor", "VERGEN_SYSINFO_CPU_VENDOR"),
    ("X-System-CPU-Brand", "VERGEN_SYSINFO_CPU_BRAND"),
    ("X-System-CPU-Name", "VERGEN_SYSINFO_CPU_NAME"),
    ("X-System-CPU-Core-Count", "VERGEN_SYSINFO_CPU_CORE_COUNT"),
    ("X-System-CPU-Frequency", "VERGEN_SYSINFO_CPU_FREQUENCY"),
    ("X-System-Total-Memory", "VERGEN_SYSINFO_TOTAL_MEMORY"),
];

const MANIFEST_GIT_KEYS: &[(&str, &str)] = &[
    ("Exists", "EXISTS"),
    ("Name", "NAME"),
    ("Description", "DESCRIPTION"),
    ("Branch", "BRANCH"),
    ("Tag", "TAG"),
    ("Commit-Short-Hash", "SHA"),
    ("Commit-Long-Hash", "LONG_HASH"),
    ("Commit-Date", "COMMIT_DATE"),
    ("Commit-Timestamp", "COMMIT_TIMESTAMP"),
    ("Commit-Message", "COMMIT_MESSAGE"),
    ("Commit-Count", "COMMIT_COUNT"),
    ("Describe", "DESCRIBE"),
    ("Dirty", "DIRTY"),
];

fn put(stamp: &mut Stamp, key: &str, value: &str) {
    stamp.insert(key.to_string(), value.to_string());
}

macro_rules! compiled_git_stamp {
    ($stamp:expr, $unused:expr, $($prefix:literal),* $(,)?) => {
        $(
            for (key, value) in [
                (concat!("VERGEN_GIT_", $prefix, "_EXISTS"), option_env!(concat!("VERGEN_GIT_", $prefix, "_EXISTS"))),
                (concat!("VERGEN_GIT_", $prefix, "_NAME"), option_env!(concat!("VERGEN_GIT_", $prefix, "_NAME"))),
                (concat!("VERGEN_GIT_", $prefix, "_DESCRIPTION"), option_env!(concat!("VERGEN_GIT_", $prefix, "_DESCRIPTION"))),
                (concat!("VERGEN_GIT_", $prefix, "_BRANCH"), option_env!(concat!("VERGEN_GIT_", $prefix, "_BRANCH"))),
                (concat!("VERGEN_GIT_", $prefix, "_TAG"), option_env!(concat!("VERGEN_GIT_", $prefix, "_TAG"))),
                (concat!("VERGEN_GIT_", $prefix, "_SHA"), option_env!(concat!("VERGEN_GIT_", $prefix, "_SHA"))),
                (concat!("VERGEN_GIT_", $prefix, "_LONG_HASH"), option_env!(concat!("VERGEN_GIT_", $prefix, "_LONG_HASH"))),
                (concat!("VERGEN_GIT_", $prefix, "_COMMIT_DATE"), option_env!(concat!("VERGEN_GIT_", $prefix, "_COMMIT_DATE"))),
                (concat!("VERGEN_GIT_", $prefix, "_COMMIT_TIMESTAMP"), option_env!(concat!("VERGEN_GIT_", $prefix, "_COMMIT_TIMESTAMP"))),
                (concat!("VERGEN_GIT_", $prefix, "_COMMIT_AUTHOR_NAME"), option_env!(concat!("VERGEN_GIT_", $prefix, "_COMMIT_AUTHOR_NAME"))),
                (concat!("VERGEN_GIT_", $prefix, "_COMMIT_AUTHOR_EMAIL"), option_env!(concat!("VERGEN_GIT_", $prefix, "_COMMIT_AUTHOR_EMAIL"))),
                (concat!("VERGEN_GIT_", $prefix, "_COMMIT_MESSAGE"), option_env!(concat!("VERGEN_GIT_", $prefix, "_COMMIT_MESSAGE"))),
                (concat!("VERGEN_GIT_", $prefix, "_COMMIT_COUNT"), option_env!(concat!("VERGEN_GIT_", $prefix, "_COMMIT_COUNT"))),
                (concat!("VERGEN_GIT_", $prefix, "_DESCRIBE"), option_env!(concat!("VERGEN_GIT_", $prefix, "_DESCRIBE"))),
                (concat!("VERGEN_GIT_", $prefix, "_DIRTY"), option_env!(concat!("VERGEN_GIT_", $prefix, "_DIRTY"))),
            ] {
                if let Some(value) = value {
                    put($stamp, key, value);
                }
            }
        )*
    };
}

fn compiled_stamp(version: &str) -> Stamp {
    let mut stamp = Stamp::new();
    put(&mut stamp, BUILD_TYPE, "Rust");
    put(&mut stamp, VERSION, version);
    for (key, value) in [
        ("VERGEN_BUILD_DATE", env!("VERGEN_BUILD_DATE")),
        (BUILD_TIMESTAMP, env!("VERGEN_BUILD_TIMESTAMP")),
        (
            "VERGEN_BUILD_TIMEZONE_NAME",
            env!("VERGEN_BUILD_TIMEZONE_NAME"),
        ),
        (
            "VERGEN_BUILD_TIMEZONE_OFFSET",
            env!("VERGEN_BUILD_TIMEZONE_OFFSET"),
        ),
        ("VERGEN_RUSTC_SEMVER", env!("VERGEN_RUSTC_SEMVER")),
        ("VERGEN_RUSTC_CHANNEL", env!("VERGEN_RUSTC_CHANNEL")),
        ("VERGEN_RUSTC_HOST_TRIPLE", env!("VERGEN_RUSTC_HOST_TRIPLE")),
        ("VERGEN_RUSTC_COMMIT_HASH", env!("VERGEN_RUSTC_COMMIT_HASH")),
        ("VERGEN_RUSTC_COMMIT_DATE", env!("VERGEN_RUSTC_COMMIT_DATE")),
        (
            "VERGEN_RUSTC_LLVM_VERSION",
            env!("VERGEN_RUSTC_LLVM_VERSION"),
        ),
        (
            "VERGEN_CARGO_TARGET_TRIPLE",
            env!("VERGEN_CARGO_TARGET_TRIPLE"),
        ),
        ("VERGEN_CARGO_FEATURES", env!("VERGEN_CARGO_FEATURES")),
        ("VERGEN_CARGO_DEBUG", env!("VERGEN_CARGO_DEBUG")),
        ("VERGEN_CARGO_OPT_LEVEL", env!("VERGEN_CARGO_OPT_LEVEL")),
        ("VERGEN_PYTHON_VERSION", env!("VERGEN_PYTHON_VERSION")),
        (
            "VERGEN_PYTHON_IMPLEMENTATION",
            env!("VERGEN_PYTHON_IMPLEMENTATION"),
        ),
        ("VERGEN_NODE_VERSION", env!("VERGEN_NODE_VERSION")),
        ("VERGEN_SYSINFO_HOST", env!("VERGEN_SYSINFO_HOST")),
        ("VERGEN_SYSINFO_USER", env!("VERGEN_SYSINFO_USER")),
        ("VERGEN_SYSINFO_NAME", env!("VERGEN_SYSINFO_NAME")),
        (
            "VERGEN_SYSINFO_OS_VERSION",
            env!("VERGEN_SYSINFO_OS_VERSION"),
        ),
        (
            "VERGEN_SYSINFO_CPU_VENDOR",
            env!("VERGEN_SYSINFO_CPU_VENDOR"),
        ),
        ("VERGEN_SYSINFO_CPU_BRAND", env!("VERGEN_SYSINFO_CPU_BRAND")),
        ("VERGEN_SYSINFO_CPU_NAME", env!("VERGEN_SYSINFO_CPU_NAME")),
        (
            "VERGEN_SYSINFO_CPU_CORE_COUNT",
            env!("VERGEN_SYSINFO_CPU_CORE_COUNT"),
        ),
        (
            "VERGEN_SYSINFO_CPU_FREQUENCY",
            env!("VERGEN_SYSINFO_CPU_FREQUENCY"),
        ),
        (
            "VERGEN_SYSINFO_TOTAL_MEMORY",
            env!("VERGEN_SYSINFO_TOTAL_MEMORY"),
        ),
    ] {
        put(&mut stamp, key, value);
    }
    td_build::invoke_add_git_sections!(&mut stamp, (), compiled_git_stamp);
    stamp
}

fn manifest_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(boolean) => Some(boolean.to_string()),
        _ => None,
    }
}

fn manifest_stamp(path: &Path, version: &str) -> Result<Stamp, String> {
    let content = fs::read_to_string(path).map_err(|error| {
        format!(
            "Could not read BUILD manifest '{}': {}",
            path.display(),
            error
        )
    })?;
    let manifest: BTreeMap<String, Value> = serde_yaml::from_str(&content).map_err(|error| {
        format!(
            "Could not parse BUILD manifest '{}': {}",
            path.display(),
            error
        )
    })?;
    let mut stamp = Stamp::new();
    put(&mut stamp, BUILD_TYPE, "Python");
    put(&mut stamp, VERSION, version);
    for (manifest_key, stamp_key) in MANIFEST_KEYS {
        if let Some(value) = manifest.get(*manifest_key).and_then(manifest_text) {
            stamp.insert(stamp_key.to_string(), value);
        }
    }
    for (key, value) in &manifest {
        let Some((prefix, field)) = key
            .strip_prefix("X-Git-")
            .and_then(|rest| rest.split_once('-'))
        else {
            continue;
        };
        let Some(value) = manifest_text(value) else {
            continue;
        };
        if field == "Commit-Author" {
            let (name, email) = match value.rsplit_once('<') {
                Some((name, email)) if email.ends_with('>') => (
                    name.trim().to_string(),
                    email.trim_end_matches('>').to_string(),
                ),
                _ => (value.clone(), "-".to_string()),
            };
            stamp.insert(format!("VERGEN_GIT_{prefix}_COMMIT_AUTHOR_NAME"), name);
            stamp.insert(format!("VERGEN_GIT_{prefix}_COMMIT_AUTHOR_EMAIL"), email);
        } else if let Some((_, suffix)) = MANIFEST_GIT_KEYS
            .iter()
            .find(|(manifest_field, _)| *manifest_field == field)
        {
            stamp.insert(format!("VERGEN_GIT_{prefix}_{suffix}"), value);
        }
    }
    Ok(stamp)
}

fn caller_stamp() -> Stamp {
    env::vars()
        .filter(|(key, _)| key.starts_with("VERGEN_") || key == BUILD_TYPE || key == VERSION)
        .collect()
}

fn build_instant(stamp: &Stamp) -> Option<DateTime<FixedOffset>> {
    stamp
        .get(BUILD_TIMESTAMP)
        .and_then(|timestamp| DateTime::parse_from_rfc3339(timestamp.trim()).ok())
}

fn latest_stamp(rust: Stamp, python: Option<Stamp>) -> Stamp {
    match python {
        Some(python) if build_instant(&python) > build_instant(&rust) => python,
        _ => rust,
    }
}

fn locate_build_manifest() -> Option<PathBuf> {
    let binary = format!("{}{}", TD_RESOURCE_BINARY, env::consts::EXE_SUFFIX);
    let program = env::current_exe().ok()?.parent()?.join(binary);
    let output = Command::new(program)
        .arg(BUILD_MANIFEST_RESOURCE)
        .output()
        .ok()
        .filter(|output| output.status.success())?;
    let path = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
    path.is_file().then_some(path)
}

fn resolve_stamp() -> Stamp {
    let caller = caller_stamp();
    let version = caller
        .get(VERSION)
        .cloned()
        .unwrap_or_else(|| "-".to_string());
    let manifest = env::var_os(TD_ABOUT_MANIFEST).map(PathBuf::from);
    let load = |path: &Path| match manifest_stamp(path, &version) {
        Ok(stamp) => Some(stamp),
        Err(error) => {
            eprintln!("Warning: {}", error);
            None
        }
    };
    if option_env!("VERGEN_TD_ON_GA") == Some("true") {
        let rust = compiled_stamp(&version);
        let python = manifest
            .or_else(locate_build_manifest)
            .and_then(|path| load(&path));
        return latest_stamp(rust, python);
    }
    match manifest {
        Some(path) => load(&path).unwrap_or(caller),
        None => caller,
    }
}

pub fn tdabout(version: &str) {
    let status = Command::new("td_about")
        .envs(compiled_stamp(version))
        .status()
        .expect("Failed to execute tdabout");

    if !status.success() {
        eprintln!("Executing tdabout failed with status: {}", status);
        std::process::exit(1);
    }
}

fn stamp_value(stamp: &Stamp, key: &str) -> String {
    stamp
        .get(key)
        .map(|value| value.trim().to_string())
        .unwrap_or_else(|| "-".to_string())
}

macro_rules! add_git_sections {
    ($stamp:expr, $git_data:expr, $($prefix:literal),* $(,)?) => {
        $(
            if stamp_value($stamp, concat!("VERGEN_GIT_", $prefix, "_EXISTS")) == "true" {
                let description = stamp_value($stamp, concat!("VERGEN_GIT_", $prefix, "_DESCRIPTION"));
                let section_title = format!("Git Information - {}", description);

                // let author_name = stamp_value($stamp, concat!("VERGEN_GIT_", $prefix, "_COMMIT_AUTHOR_NAME"));
                // let author_email = stamp_value($stamp, concat!("VERGEN_GIT_", $prefix, "_COMMIT_AUTHOR_EMAIL"));
                // let commit_author = format!("{} <{}>", author_name.trim(), author_email.trim());

                $git_data.push((
                    section_title.clone(),
                    vec![
                        ("Branch".to_string(), stamp_value($stamp, concat!("VERGEN_GIT_", $prefix, "_BRANCH"))),
                        ("Tag".to_string(), stamp_value($stamp, concat!("VERGEN_GIT_", $prefix, "_TAG"))),
                        ("Commit Short Hash".to_string(), stamp_value($stamp, concat!("VERGEN_GIT_", $prefix, "_SHA"))),
                        ("Commit Long Hash".to_string(), stamp_value($stamp, concat!("VERGEN_GIT_", $prefix, "_LONG_HASH"))),
                        ("Commit Date".to_string(), stamp_value($stamp, concat!("VERGEN_GIT_", $prefix, "_COMMIT_DATE"))),
                        ("Commit Timestamp".to_string(), stamp_value($stamp, concat!("VERGEN_GIT_", $prefix, "_COMMIT_TIMESTAMP"))),
                        // ("Commit Author".to_string(), commit_author),
                        // ("Commit Message".to_string(), stamp_value($stamp, concat!("VERGEN_GIT_", $prefix, "_COMMIT_MESSAGE"))),
                        ("Commit Count".to_string(), stamp_value($stamp, concat!("VERGEN_GIT_", $prefix, "_COMMIT_COUNT"))),
                        ("Describe".to_string(), stamp_value($stamp, concat!("VERGEN_GIT_", $prefix, "_DESCRIBE"))),
                        ("Dirty".to_string(), stamp_value($stamp, concat!("VERGEN_GIT_", $prefix, "_DIRTY"))),
                    ],
                ));
            }
        )*
    };
}

pub fn show_build_metadata() {
    let stamp = resolve_stamp();
    let value = |key: &str| stamp_value(&stamp, key);

    #[cfg(not(windows))]
    let use_colors = supports_color::on(supports_color::Stream::Stdout).is_some();
    #[cfg(windows)]
    let use_colors = false;

    fn title_case(title: &str) -> String {
        title
            .split_whitespace()
            .map(|word| {
                let mut chars = word.chars();
                match chars.next() {
                    None => String::new(),
                    Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn pad_to_width(content: &str, width: usize) -> String {
        let used = content.width();
        format!("{content}{}", " ".repeat(width.saturating_sub(used)))
    }

    let version = value("TD_VERSION");
    let build_type = value("TD_VERGEN_BUILD_TYPE");
    let _is_python = build_type == "Python";
    let is_rust = build_type == "Rust";

    let header_line_about = "About";
    let header_line_version = format!("Tabsdata Version {}", version);

    #[cfg(feature = "enterprise")]
    let edition = "Enterprise";
    #[cfg(not(feature = "enterprise"))]
    let edition = "Open Source";
    let header_line_edition = format!("Edition: {}", edition);

    #[cfg(any(test, feature = "mock-env"))]
    let mode = "Development (mock-env feature enabled)";
    #[cfg(not(any(test, feature = "mock-env")))]
    let mode = "Production";
    let header_line_mode = format!("Mode: {}", mode);

    // Build Information
    let build_timezone_name = value("VERGEN_BUILD_TIMEZONE_NAME");
    let build_timezone_offset = value("VERGEN_BUILD_TIMEZONE_OFFSET");
    let build_timezone = format!(
        "{} ({})",
        build_timezone_name.trim(),
        build_timezone_offset.trim()
    );
    let build_date_utc = value("VERGEN_BUILD_DATE");
    let build_timestamp_utc = value("VERGEN_BUILD_TIMESTAMP");
    let (build_timestamp_local, build_date_local) =
        if let Ok(dt_utc) = chrono::DateTime::parse_from_rfc3339(&build_timestamp_utc) {
            if let Ok(offset) = build_timezone_offset.parse::<FixedOffset>() {
                let dt_local = dt_utc.with_timezone(&offset);
                (
                    dt_local.to_rfc3339(),
                    dt_local.format("%Y-%m-%d").to_string(),
                )
            } else {
                ("-".to_string(), "-".to_string())
            }
        } else {
            ("-".to_string(), "-".to_string())
        };

    // Rust Information
    let rustc_semver = value("VERGEN_RUSTC_SEMVER");
    let rustc_channel = value("VERGEN_RUSTC_CHANNEL");
    let rustc_host_triple = value("VERGEN_RUSTC_HOST_TRIPLE");
    let rustc_commit_hash = value("VERGEN_RUSTC_COMMIT_HASH");
    let rustc_commit_date = value("VERGEN_RUSTC_COMMIT_DATE");
    let rustc_llvm_version = value("VERGEN_RUSTC_LLVM_VERSION");

    // Cargo Information
    let cargo_target_triple = value("VERGEN_CARGO_TARGET_TRIPLE");
    let cargo_features = value("VERGEN_CARGO_FEATURES");
    let cargo_debug = value("VERGEN_CARGO_DEBUG");
    let cargo_opt_level = value("VERGEN_CARGO_OPT_LEVEL");

    // Python Information
    let python_version = value("VERGEN_PYTHON_VERSION");
    let python_implementation = value("VERGEN_PYTHON_IMPLEMENTATION");

    // Node Information
    let node_version = value("VERGEN_NODE_VERSION");

    // System Information
    let sysinfo_host = value("VERGEN_SYSINFO_HOST");
    let sysinfo_user = value("VERGEN_SYSINFO_USER");
    let sysinfo_name = value("VERGEN_SYSINFO_NAME");
    let sysinfo_os_version = value("VERGEN_SYSINFO_OS_VERSION");
    let sysinfo_cpu_vendor = value("VERGEN_SYSINFO_CPU_VENDOR");
    let sysinfo_cpu_brand = value("VERGEN_SYSINFO_CPU_BRAND");
    let sysinfo_cpu_name = value("VERGEN_SYSINFO_CPU_NAME");
    let sysinfo_cpu_core_count = value("VERGEN_SYSINFO_CPU_CORE_COUNT");
    let cpu_frequency_val = value("VERGEN_SYSINFO_CPU_FREQUENCY");
    let cpu_frequency = match cpu_frequency_val.parse::<u64>() {
        Ok(frequency) if frequency > 0 => format!("{} MHz", frequency),
        _ => "-".to_string(),
    };
    let total_memory = value("VERGEN_SYSINFO_TOTAL_MEMORY");

    let build_info_title = if is_rust {
        "Build Information"
    } else {
        "Package Information"
    };

    let build_label = if is_rust { "Build" } else { "Package" };

    let build_date_utc_label = format!("{} Date (UTC)", build_label);
    let build_timestamp_utc_label = format!("{} Timestamp (UTC)", build_label);
    let build_date_local_label = format!("{} Date (Local)", build_label);
    let build_timestamp_local_label = format!("{} Timestamp (Local)", build_label);
    let build_timezone_label = format!("{} Timezone", build_label);

    let mut sections = vec![(
        build_info_title,
        vec![
            (build_date_utc_label.as_str(), build_date_utc.trim()),
            (
                build_timestamp_utc_label.as_str(),
                build_timestamp_utc.trim(),
            ),
            (build_date_local_label.as_str(), build_date_local.as_str()),
            (
                build_timestamp_local_label.as_str(),
                build_timestamp_local.as_str(),
            ),
            (build_timezone_label.as_str(), build_timezone.as_str()),
        ],
    )];

    let mut git_sections: Vec<(String, Vec<(String, String)>)> = Vec::new();
    td_build::invoke_add_git_sections!(&stamp, git_sections, add_git_sections);
    for (title, items) in &git_sections {
        sections.push((
            title.as_str(),
            items
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect(),
        ));
    }

    sections.extend(vec![(
        "Rust Information",
        vec![
            ("Rust Version", rustc_semver.trim()),
            ("Rust Channel", rustc_channel.trim()),
            ("Rust Host Triple", rustc_host_triple.trim()),
            ("Rust Commit Hash", rustc_commit_hash.trim()),
            ("Rust Commit Date", rustc_commit_date.trim()),
            ("LLVM Version", rustc_llvm_version.trim()),
        ],
    )]);

    if is_rust {
        sections.push((
            "Cargo Information",
            vec![
                ("Target Triple", cargo_target_triple.trim()),
                ("Features", cargo_features.trim()),
                ("Debug", cargo_debug.trim()),
                ("Optimization Level", cargo_opt_level.trim()),
            ],
        ));
    }

    sections.push((
        "Python Information",
        vec![
            ("Python Version", python_version.trim()),
            ("Python Implementation", python_implementation.trim()),
        ],
    ));

    sections.push((
        "Node Information",
        vec![("Node Version", node_version.trim())],
    ));

    sections.push((
        "Build System Information",
        vec![
            ("Host", sysinfo_host.trim()),
            ("User", sysinfo_user.trim()),
            ("OS Name", sysinfo_name.trim()),
            ("OS Version", sysinfo_os_version.trim()),
            ("CPU Vendor", sysinfo_cpu_vendor.trim()),
            ("CPU Brand", sysinfo_cpu_brand.trim()),
            ("CPU Name", sysinfo_cpu_name.trim()),
            ("CPU Core Count", sysinfo_cpu_core_count.trim()),
            ("CPU Frequency", cpu_frequency.trim()),
            ("Total Memory", total_memory.trim()),
        ],
    ));

    let max_caption_length = sections
        .iter()
        .flat_map(|(_, items)| items.iter().map(|(key, _)| key.width()))
        .max()
        .unwrap_or(0);

    let max_value_length = sections
        .iter()
        .flat_map(|(_, items)| items.iter().map(|(_, value)| value.width()))
        .max()
        .unwrap_or(0);

    let max_title_length = sections
        .iter()
        .map(|(title, _)| title.width())
        .max()
        .unwrap_or(0);

    let max_header_length = header_line_about
        .chars()
        .count()
        .max(header_line_version.width())
        .max(header_line_edition.width())
        .max(header_line_mode.width());

    let left_padding = 1;
    let right_padding = 1;
    let separator_space = 2;
    let content_width = max_caption_length + separator_space + max_value_length;

    let box_width =
        left_padding + content_width.max(max_title_length).max(max_header_length) + right_padding;
    let inner_width = box_width - left_padding - right_padding;

    let top_border = if use_colors {
        format!("╭{}╮", "─".repeat(box_width))
            .blue()
            .bold()
            .to_string()
    } else {
        format!("╭{}╮", "─".repeat(box_width))
    };

    let bottom_border = if use_colors {
        format!("╰{}╯", "─".repeat(box_width))
            .blue()
            .bold()
            .to_string()
    } else {
        format!("╰{}╯", "─".repeat(box_width))
    };

    let horizontal_line = if use_colors {
        format!("├{}┤", "─".repeat(box_width))
            .blue()
            .bold()
            .to_string()
    } else {
        format!("├{}┤", "─".repeat(box_width))
    };

    let border_char = if use_colors {
        "│".blue().bold().to_string()
    } else {
        "│".to_string()
    };

    println!("\n{top_border}");

    let header_about_length = header_line_about.width();
    let header_about_padding_left = (inner_width - header_about_length) / 2;
    let header_about_content = format!(
        "{}{}{}",
        " ".repeat(header_about_padding_left),
        header_line_about,
        " ".repeat(
            inner_width
                .saturating_sub(header_about_padding_left)
                .saturating_sub(header_about_length),
        )
    );
    let header_about_padded = pad_to_width(&header_about_content, inner_width);
    let header_about_display = if use_colors {
        header_about_padded.bright_magenta().bold().to_string()
    } else {
        header_about_padded
    };
    println!("{} {} {}", border_char, header_about_display, border_char);

    let header_version_length = header_line_version.width();
    let header_version_padding_left = (inner_width - header_version_length) / 2;
    let header_version_content = format!(
        "{}{}{}",
        " ".repeat(header_version_padding_left),
        header_line_version,
        " ".repeat(
            inner_width
                .saturating_sub(header_version_padding_left)
                .saturating_sub(header_version_length),
        )
    );
    let header_version_padded = pad_to_width(&header_version_content, inner_width);
    let header_version_display = if use_colors {
        header_version_padded.bright_magenta().bold().to_string()
    } else {
        header_version_padded
    };
    println!("{} {} {}", border_char, header_version_display, border_char);

    let header_edition_length = header_line_edition.width();
    let header_edition_padding_left = (inner_width - header_edition_length) / 2;
    let header_edition_content = format!(
        "{}{}{}",
        " ".repeat(header_edition_padding_left),
        header_line_edition,
        " ".repeat(
            inner_width
                .saturating_sub(header_edition_padding_left)
                .saturating_sub(header_edition_length),
        )
    );
    let header_edition_padded = pad_to_width(&header_edition_content, inner_width);
    let header_edition_display = if use_colors {
        header_edition_padded.bright_magenta().bold().to_string()
    } else {
        header_edition_padded
    };
    println!("{} {} {}", border_char, header_edition_display, border_char);

    let header_mode_length = header_line_mode.width();
    let header_mode_padding_left = (inner_width - header_mode_length) / 2;
    let header_mode_content = format!(
        "{}{}{}",
        " ".repeat(header_mode_padding_left),
        header_line_mode,
        " ".repeat(
            inner_width
                .saturating_sub(header_mode_padding_left)
                .saturating_sub(header_mode_length),
        )
    );
    let header_mode_padded = pad_to_width(&header_mode_content, inner_width);
    let header_mode_display = if use_colors {
        header_mode_padded.bright_magenta().bold().to_string()
    } else {
        header_mode_padded
    };
    println!("{} {} {}", border_char, header_mode_display, border_char);

    println!("{}", horizontal_line);

    for (idx, (title, items)) in sections.iter().enumerate() {
        let title_formatted = title_case(title);
        let title_padded = pad_to_width(&title_formatted, inner_width);
        let title_display = if use_colors {
            title_padded.bright_yellow().bold().to_string()
        } else {
            title_padded
        };
        println!("{} {} {}", border_char, title_display, border_char);

        let blank_line = pad_to_width("", inner_width);
        println!("{} {} {}", border_char, blank_line, border_char);

        for (key, value) in items {
            let key_len = key.width();
            let value_len = value.width();
            let dots_needed = max_caption_length.saturating_sub(key_len);
            let caption_with_dots = format!("{}{}", key, ".".repeat(dots_needed));

            let content_line = format!("{}: {}", caption_with_dots, value);
            let content_padded = pad_to_width(&content_line, inner_width);

            let content_display = if use_colors {
                let caption_colored = caption_with_dots.bright_cyan().bold().to_string();
                let value_colored = value.truecolor(194, 255, 38).to_string();
                let colored_line = format!("{}: {}", caption_colored, value_colored);
                format!(
                    "{}{}",
                    colored_line,
                    " ".repeat(
                        inner_width
                            .saturating_sub(max_caption_length)
                            .saturating_sub(2)
                            .saturating_sub(value_len),
                    )
                )
            } else {
                content_padded
            };

            println!("{} {} {}", border_char, content_display, border_char);
        }

        if idx < sections.len() - 1 {
            println!("{}", horizontal_line);
        }
    }

    println!("{bottom_border}\n");
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST: &str = r#"
X-Build-Date-UTC: '2026-09-22'
X-Build-Timestamp-UTC: '2026-09-22T21:14:23.750591000Z'
X-Git-TABSDATA_OS-Exists: 'true'
X-Git-TABSDATA_OS-Commit-Short-Hash: bdd6ab8cf
X-Git-TABSDATA_OS-Commit-Author: Dimas <dragon@tabsdata.com>
X-Git-TABSDATA_OS-Commit-Count: '3185'
X-Git-TABSDATA_OS-Dirty: 'false'
X-System-CPU-Frequency: '-'
X-System-CPU-Core-Count: 5
"#;

    fn manifest() -> Stamp {
        let path = env::temp_dir().join(format!("td-common-about-{}.yaml", std::process::id()));
        fs::write(&path, MANIFEST).unwrap();
        let stamp = manifest_stamp(&path, "2.1.0").unwrap();
        fs::remove_file(path).unwrap();
        stamp
    }

    #[test]
    fn manifest_keys_map_to_stamp_keys() {
        let stamp = manifest();
        assert_eq!(stamp_value(&stamp, BUILD_TYPE), "Python");
        assert_eq!(stamp_value(&stamp, VERSION), "2.1.0");
        assert_eq!(
            stamp_value(&stamp, BUILD_TIMESTAMP),
            "2026-09-22T21:14:23.750591000Z"
        );
        assert_eq!(
            stamp_value(&stamp, "VERGEN_GIT_TABSDATA_OS_SHA"),
            "bdd6ab8cf"
        );
        assert_eq!(
            stamp_value(&stamp, "VERGEN_GIT_TABSDATA_OS_COMMIT_AUTHOR_NAME"),
            "Dimas"
        );
        assert_eq!(
            stamp_value(&stamp, "VERGEN_GIT_TABSDATA_OS_COMMIT_AUTHOR_EMAIL"),
            "dragon@tabsdata.com"
        );
        assert_eq!(stamp_value(&stamp, "VERGEN_SYSINFO_CPU_CORE_COUNT"), "5");
        assert_eq!(stamp_value(&stamp, "VERGEN_CARGO_DEBUG"), "-");
    }

    #[test]
    fn latest_stamp_wins() {
        let python = manifest();
        let mut older = Stamp::new();
        put(&mut older, BUILD_TYPE, "Rust");
        put(
            &mut older,
            BUILD_TIMESTAMP,
            "2026-09-22T20:51:48.622032000Z",
        );
        let mut newer = older.clone();
        put(
            &mut newer,
            BUILD_TIMESTAMP,
            "2026-09-22T22:00:00.000000000Z",
        );
        assert_eq!(
            stamp_value(
                &latest_stamp(older.clone(), Some(python.clone())),
                BUILD_TYPE
            ),
            "Python"
        );
        assert_eq!(
            stamp_value(&latest_stamp(newer, Some(python)), BUILD_TYPE),
            "Rust"
        );
        assert_eq!(stamp_value(&latest_stamp(older, None), BUILD_TYPE), "Rust");
    }
}
