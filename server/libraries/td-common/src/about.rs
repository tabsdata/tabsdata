//
// Copyright 2025 Tabs Data Inc.
//

use chrono::FixedOffset;
use colored::Colorize;
use std::env;
use std::process::Command;
use supports_color;

pub fn tdabout(version: &str) {
    // Setting env vars is not thread-safe; use with care.
    unsafe {
        env::set_var("TD_VERGEN_BUILD_TYPE", "Rust");
        env::set_var("TD_VERSION", version);

        // Build Information
        env::set_var("VERGEN_BUILD_DATE", env!("VERGEN_BUILD_DATE"));
        env::set_var("VERGEN_BUILD_TIMESTAMP", env!("VERGEN_BUILD_TIMESTAMP"));
        env::set_var(
            "VERGEN_BUILD_TIMEZONE_NAME",
            env!("VERGEN_BUILD_TIMEZONE_NAME"),
        );
        env::set_var(
            "VERGEN_BUILD_TIMEZONE_OFFSET",
            env!("VERGEN_BUILD_TIMEZONE_OFFSET"),
        );

        // Git Information
        macro_rules! set_repo_env_vars {
            ($dummy1:expr, $dummy2:expr, $($prefix:literal),* $(,)?) => {
                $(
                    if let Some(name) = option_env!(concat!("VERGEN_GIT_", $prefix, "_NAME")) {
                        env::set_var(concat!("VERGEN_GIT_", $prefix, "_NAME"), name);
                    }
                    if let Some(desc) = option_env!(concat!("VERGEN_GIT_", $prefix, "_DESCRIPTION")) {
                        env::set_var(concat!("VERGEN_GIT_", $prefix, "_DESCRIPTION"), desc);
                    }
                    if let Some(exists) = option_env!(concat!("VERGEN_GIT_", $prefix, "_EXISTS")) {
                        env::set_var(concat!("VERGEN_GIT_", $prefix, "_EXISTS"), exists);
                        if exists == "true" {
                            if let Some(val) = option_env!(concat!("VERGEN_GIT_", $prefix, "_BRANCH")) {
                                env::set_var(concat!("VERGEN_GIT_", $prefix, "_BRANCH"), val);
                            }
                            if let Some(val) = option_env!(concat!("VERGEN_GIT_", $prefix, "_TAG")) {
                                env::set_var(concat!("VERGEN_GIT_", $prefix, "_TAG"), val);
                            }
                            if let Some(val) = option_env!(concat!("VERGEN_GIT_", $prefix, "_SHA")) {
                                env::set_var(concat!("VERGEN_GIT_", $prefix, "_SHA"), val);
                            }
                            if let Some(val) = option_env!(concat!("VERGEN_GIT_", $prefix, "_LONG_HASH")) {
                                env::set_var(concat!("VERGEN_GIT_", $prefix, "_LONG_HASH"), val);
                            }
                            if let Some(val) = option_env!(concat!("VERGEN_GIT_", $prefix, "_COMMIT_DATE")) {
                                env::set_var(concat!("VERGEN_GIT_", $prefix, "_COMMIT_DATE"), val);
                            }
                            if let Some(val) = option_env!(concat!("VERGEN_GIT_", $prefix, "_COMMIT_TIMESTAMP")) {
                                env::set_var(concat!("VERGEN_GIT_", $prefix, "_COMMIT_TIMESTAMP"), val);
                            }
                            if let Some(val) = option_env!(concat!("VERGEN_GIT_", $prefix, "_COMMIT_AUTHOR_NAME")) {
                                env::set_var(concat!("VERGEN_GIT_", $prefix, "_COMMIT_AUTHOR_NAME"), val);
                            }
                            if let Some(val) = option_env!(concat!("VERGEN_GIT_", $prefix, "_COMMIT_AUTHOR_EMAIL")) {
                                env::set_var(concat!("VERGEN_GIT_", $prefix, "_COMMIT_AUTHOR_EMAIL"), val);
                            }
                            if let Some(val) = option_env!(concat!("VERGEN_GIT_", $prefix, "_COMMIT_MESSAGE")) {
                                env::set_var(concat!("VERGEN_GIT_", $prefix, "_COMMIT_MESSAGE"), val);
                            }
                            if let Some(val) = option_env!(concat!("VERGEN_GIT_", $prefix, "_COMMIT_COUNT")) {
                                env::set_var(concat!("VERGEN_GIT_", $prefix, "_COMMIT_COUNT"), val);
                            }
                            if let Some(val) = option_env!(concat!("VERGEN_GIT_", $prefix, "_DESCRIBE")) {
                                env::set_var(concat!("VERGEN_GIT_", $prefix, "_DESCRIBE"), val);
                            }
                            if let Some(val) = option_env!(concat!("VERGEN_GIT_", $prefix, "_DIRTY")) {
                                env::set_var(concat!("VERGEN_GIT_", $prefix, "_DIRTY"), val);
                            }
                        }
                    }
                )*
            };
        }
        td_build::invoke_add_git_sections!((), (), set_repo_env_vars);

        // Rust Information
        env::set_var("VERGEN_RUSTC_SEMVER", env!("VERGEN_RUSTC_SEMVER"));
        env::set_var("VERGEN_RUSTC_CHANNEL", env!("VERGEN_RUSTC_CHANNEL"));
        env::set_var("VERGEN_RUSTC_HOST_TRIPLE", env!("VERGEN_RUSTC_HOST_TRIPLE"));
        env::set_var("VERGEN_RUSTC_COMMIT_HASH", env!("VERGEN_RUSTC_COMMIT_HASH"));
        env::set_var("VERGEN_RUSTC_COMMIT_DATE", env!("VERGEN_RUSTC_COMMIT_DATE"));
        env::set_var(
            "VERGEN_RUSTC_LLVM_VERSION",
            env!("VERGEN_RUSTC_LLVM_VERSION"),
        );

        // Cargo Information
        env::set_var(
            "VERGEN_CARGO_TARGET_TRIPLE",
            env!("VERGEN_CARGO_TARGET_TRIPLE"),
        );
        env::set_var("VERGEN_CARGO_FEATURES", env!("VERGEN_CARGO_FEATURES"));
        env::set_var("VERGEN_CARGO_DEBUG", env!("VERGEN_CARGO_DEBUG"));
        env::set_var("VERGEN_CARGO_OPT_LEVEL", env!("VERGEN_CARGO_OPT_LEVEL"));

        // Python Information
        env::set_var("VERGEN_PYTHON_VERSION", env!("VERGEN_PYTHON_VERSION"));
        env::set_var(
            "VERGEN_PYTHON_IMPLEMENTATION",
            env!("VERGEN_PYTHON_IMPLEMENTATION"),
        );

        // Node Information
        env::set_var("VERGEN_NODE_VERSION", env!("VERGEN_NODE_VERSION"));

        // System Information
        env::set_var("VERGEN_SYSINFO_HOST", env!("VERGEN_SYSINFO_HOST"));
        env::set_var("VERGEN_SYSINFO_USER", env!("VERGEN_SYSINFO_USER"));
        env::set_var("VERGEN_SYSINFO_NAME", env!("VERGEN_SYSINFO_NAME"));
        env::set_var(
            "VERGEN_SYSINFO_OS_VERSION",
            env!("VERGEN_SYSINFO_OS_VERSION"),
        );
        env::set_var(
            "VERGEN_SYSINFO_CPU_VENDOR",
            env!("VERGEN_SYSINFO_CPU_VENDOR"),
        );
        env::set_var("VERGEN_SYSINFO_CPU_BRAND", env!("VERGEN_SYSINFO_CPU_BRAND"));
        env::set_var("VERGEN_SYSINFO_CPU_NAME", env!("VERGEN_SYSINFO_CPU_NAME"));
        env::set_var(
            "VERGEN_SYSINFO_CPU_CORE_COUNT",
            env!("VERGEN_SYSINFO_CPU_CORE_COUNT"),
        );
        env::set_var(
            "VERGEN_SYSINFO_CPU_FREQUENCY",
            env!("VERGEN_SYSINFO_CPU_FREQUENCY"),
        );
        env::set_var(
            "VERGEN_SYSINFO_TOTAL_MEMORY",
            env!("VERGEN_SYSINFO_TOTAL_MEMORY"),
        );
    }

    let status = Command::new("tdabout")
        .status()
        .expect("Failed to execute tdabout");

    if !status.success() {
        eprintln!("Executing tdabout failed with status: {}", status);
        std::process::exit(1);
    }
}

macro_rules! add_git_sections {
    ($sections:expr, $git_data:expr, $($prefix:literal),* $(,)?) => {
        $(
            if env::var(concat!("VERGEN_GIT_", $prefix, "_EXISTS")).unwrap_or_default() == "true" {
                let description = env::var(concat!("VERGEN_GIT_", $prefix, "_DESCRIPTION")).unwrap_or_default();
                let section_title = format!("Git Information - {}", description);

                let author_name = env::var(concat!("VERGEN_GIT_", $prefix, "_COMMIT_AUTHOR_NAME")).unwrap_or_default();
                let author_email = env::var(concat!("VERGEN_GIT_", $prefix, "_COMMIT_AUTHOR_EMAIL")).unwrap_or_default();
                let commit_author = format!("{} <{}>", author_name.trim(), author_email.trim());

                $git_data.push((
                    section_title.clone(),
                    vec![
                        ("Branch".to_string(), env::var(concat!("VERGEN_GIT_", $prefix, "_BRANCH")).unwrap_or_default().trim().to_string()),
                        ("Tag".to_string(), env::var(concat!("VERGEN_GIT_", $prefix, "_TAG")).unwrap_or_default().trim().to_string()),
                        ("Commit Short Hash".to_string(), env::var(concat!("VERGEN_GIT_", $prefix, "_SHA")).unwrap_or_default().trim().to_string()),
                        ("Commit Long Hash".to_string(), env::var(concat!("VERGEN_GIT_", $prefix, "_LONG_HASH")).unwrap_or_default().trim().to_string()),
                        ("Commit Date".to_string(), env::var(concat!("VERGEN_GIT_", $prefix, "_COMMIT_DATE")).unwrap_or_default().trim().to_string()),
                        ("Commit Timestamp".to_string(), env::var(concat!("VERGEN_GIT_", $prefix, "_COMMIT_TIMESTAMP")).unwrap_or_default().trim().to_string()),
                        ("Commit Author".to_string(), commit_author),
                        ("Commit Message".to_string(), env::var(concat!("VERGEN_GIT_", $prefix, "_COMMIT_MESSAGE")).unwrap_or_default().trim().to_string()),
                        ("Commit Count".to_string(), env::var(concat!("VERGEN_GIT_", $prefix, "_COMMIT_COUNT")).unwrap_or_default().trim().to_string()),
                        ("Describe".to_string(), env::var(concat!("VERGEN_GIT_", $prefix, "_DESCRIBE")).unwrap_or_default().trim().to_string()),
                        ("Dirty".to_string(), env::var(concat!("VERGEN_GIT_", $prefix, "_DIRTY")).unwrap_or_default().trim().to_string()),
                    ],
                ));
            }
        )*
    };
}

pub fn show_build_metadata() {
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
        format!("{:<width$}", content, width = width)
    }

    let version = env::var("TD_VERSION").unwrap_or_else(|_| "?".to_string());
    let build_type = env::var("TD_VERGEN_BUILD_TYPE").unwrap_or_else(|_| "?".to_string());
    let _is_python = build_type == "Python";
    let is_rust = build_type == "Rust";

    let header_line_about = "About";
    let header_line_version = format!("Tabsdata Version {}", version);

    // Build Information
    let build_timezone_name = env::var("VERGEN_BUILD_TIMEZONE_NAME").unwrap_or_default();
    let build_timezone_offset = env::var("VERGEN_BUILD_TIMEZONE_OFFSET").unwrap_or_default();
    let build_timezone = format!(
        "{} ({})",
        build_timezone_name.trim(),
        build_timezone_offset.trim()
    );
    let build_date_utc = env::var("VERGEN_BUILD_DATE").unwrap_or_default();
    let build_timestamp_utc = env::var("VERGEN_BUILD_TIMESTAMP").unwrap_or_default();
    let (build_timestamp_local, build_date_local) =
        if let Ok(dt_utc) = chrono::DateTime::parse_from_rfc3339(&build_timestamp_utc) {
            if let Ok(offset) = build_timezone_offset.parse::<FixedOffset>() {
                let dt_local = dt_utc.with_timezone(&offset);
                (
                    dt_local.to_rfc3339(),
                    dt_local.format("%Y-%m-%d").to_string(),
                )
            } else {
                ("?".to_string(), "?".to_string())
            }
        } else {
            ("?".to_string(), "?".to_string())
        };

    // Rust Information
    let rustc_semver = env::var("VERGEN_RUSTC_SEMVER").unwrap_or_default();
    let rustc_channel = env::var("VERGEN_RUSTC_CHANNEL").unwrap_or_default();
    let rustc_host_triple = env::var("VERGEN_RUSTC_HOST_TRIPLE").unwrap_or_default();
    let rustc_commit_hash = env::var("VERGEN_RUSTC_COMMIT_HASH").unwrap_or_default();
    let rustc_commit_date = env::var("VERGEN_RUSTC_COMMIT_DATE").unwrap_or_default();
    let rustc_llvm_version = env::var("VERGEN_RUSTC_LLVM_VERSION").unwrap_or_default();

    // Cargo Information
    let cargo_target_triple = env::var("VERGEN_CARGO_TARGET_TRIPLE").unwrap_or_default();
    let cargo_features = env::var("VERGEN_CARGO_FEATURES").unwrap_or_default();
    let cargo_debug = env::var("VERGEN_CARGO_DEBUG").unwrap_or_default();
    let cargo_opt_level = env::var("VERGEN_CARGO_OPT_LEVEL").unwrap_or_default();

    // Python Information
    let python_version = env::var("VERGEN_PYTHON_VERSION").unwrap_or_default();
    let python_implementation = env::var("VERGEN_PYTHON_IMPLEMENTATION").unwrap_or_default();

    // Node Information
    let node_version = env::var("VERGEN_NODE_VERSION").unwrap_or_default();

    // System Information
    let sysinfo_host = env::var("VERGEN_SYSINFO_HOST").unwrap_or_default();
    let sysinfo_user = env::var("VERGEN_SYSINFO_USER").unwrap_or_default();
    let sysinfo_name = env::var("VERGEN_SYSINFO_NAME").unwrap_or_default();
    let sysinfo_os_version = env::var("VERGEN_SYSINFO_OS_VERSION").unwrap_or_default();
    let sysinfo_cpu_vendor = env::var("VERGEN_SYSINFO_CPU_VENDOR").unwrap_or_default();
    let sysinfo_cpu_brand = env::var("VERGEN_SYSINFO_CPU_BRAND").unwrap_or_default();
    let sysinfo_cpu_name = env::var("VERGEN_SYSINFO_CPU_NAME").unwrap_or_default();
    let sysinfo_cpu_core_count = env::var("VERGEN_SYSINFO_CPU_CORE_COUNT").unwrap_or_default();
    let cpu_frequency_val = env::var("VERGEN_SYSINFO_CPU_FREQUENCY").unwrap_or_default();
    let cpu_frequency = format!("{} MHz", cpu_frequency_val);
    let total_memory = env::var("VERGEN_SYSINFO_TOTAL_MEMORY").unwrap_or_default();

    let mut sections = vec![(
        "Build Information",
        vec![
            ("Build Date (UTC)", build_date_utc.trim()),
            ("Build Timestamp (UTC)", build_timestamp_utc.trim()),
            ("Build Date (Local)", build_date_local.as_str()),
            ("Build Timestamp (Local)", build_timestamp_local.as_str()),
            ("Build Timezone", build_timezone.as_str()),
        ],
    )];

    let mut git_sections: Vec<(String, Vec<(String, String)>)> = Vec::new();
    td_build::invoke_add_git_sections!(sections, git_sections, add_git_sections);
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
        "System Information",
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
        .flat_map(|(_, items)| items.iter().map(|(key, _)| key.chars().count()))
        .max()
        .unwrap_or(0);

    let max_value_length = sections
        .iter()
        .flat_map(|(_, items)| items.iter().map(|(_, value)| value.chars().count()))
        .max()
        .unwrap_or(0);

    let max_title_length = sections
        .iter()
        .map(|(title, _)| title.chars().count())
        .max()
        .unwrap_or(0);

    let max_header_length = header_line_about
        .chars()
        .count()
        .max(header_line_version.chars().count());

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

    let header_about_length = header_line_about.chars().count();
    let header_about_padding_left = (inner_width - header_about_length) / 2;
    let header_about_content = format!(
        "{}{}{}",
        " ".repeat(header_about_padding_left),
        header_line_about,
        " ".repeat(inner_width - header_about_padding_left - header_about_length)
    );
    let header_about_padded = pad_to_width(&header_about_content, inner_width);
    let header_about_display = if use_colors {
        header_about_padded.bright_magenta().bold().to_string()
    } else {
        header_about_padded
    };
    println!("{} {} {}", border_char, header_about_display, border_char);

    let header_version_length = header_line_version.chars().count();
    let header_version_padding_left = (inner_width - header_version_length) / 2;
    let header_version_content = format!(
        "{}{}{}",
        " ".repeat(header_version_padding_left),
        &header_line_version,
        " ".repeat(inner_width - header_version_padding_left - header_version_length)
    );
    let header_version_padded = pad_to_width(&header_version_content, inner_width);
    let header_version_display = if use_colors {
        header_version_padded.bright_magenta().bold().to_string()
    } else {
        header_version_padded
    };
    println!("{} {} {}", border_char, header_version_display, border_char);

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
            let key_len = key.chars().count();
            let value_len = value.chars().count();
            let dots_needed = max_caption_length - key_len;
            let caption_with_dots = format!("{}{}", key, ".".repeat(dots_needed));

            let content_line = format!("{}: {}", caption_with_dots, value);
            let content_padded = pad_to_width(&content_line, inner_width);

            let content_display = if use_colors {
                let caption_colored = caption_with_dots.bright_cyan().bold().to_string();
                let value_colored = value.truecolor(251, 175, 79).to_string();
                let colored_line = format!("{}: {}", caption_colored, value_colored);
                format!(
                    "{}{}",
                    colored_line,
                    " ".repeat(inner_width - max_caption_length - 2 - value_len)
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
