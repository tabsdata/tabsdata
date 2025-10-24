//
// Copyright 2025 Tabs Data Inc.
//

use chrono::FixedOffset;
use colored::Colorize;
use supports_color;

macro_rules! add_git_sections {
    ($sections:expr, $git_data:expr, $($prefix:literal),* $(,)?) => {
        $(
            if option_env!(concat!("VERGEN_GIT_", $prefix, "_EXISTS")) == Some("true") {
                let section_title = format!(
                    "Git Information - {}",
                    env!(concat!("VERGEN_GIT_", $prefix, "_DESCRIPTION"))
                );
                let commit_author = format!(
                    "{} <{}>",
                    env!(concat!("VERGEN_GIT_", $prefix, "_COMMIT_AUTHOR_NAME")),
                    env!(concat!("VERGEN_GIT_", $prefix, "_COMMIT_AUTHOR_EMAIL"))
                );

                $git_data.push((
                    section_title.clone(),
                    vec![
                        ("Branch".to_string(), env!(concat!("VERGEN_GIT_", $prefix, "_BRANCH")).trim().to_string()),
                        ("Tag".to_string(), env!(concat!("VERGEN_GIT_", $prefix, "_TAG")).trim().to_string()),
                        ("Commit Short Hash".to_string(), env!(concat!("VERGEN_GIT_", $prefix, "_SHA")).trim().to_string()),
                        ("Commit Long Hash".to_string(), env!(concat!("VERGEN_GIT_", $prefix, "_LONG_HASH")).trim().to_string()),
                        ("Commit Date".to_string(), env!(concat!("VERGEN_GIT_", $prefix, "_COMMIT_DATE")).trim().to_string()),
                        ("Commit Timestamp".to_string(), env!(concat!("VERGEN_GIT_", $prefix, "_COMMIT_TIMESTAMP")).trim().to_string()),
                        ("Commit Author".to_string(), commit_author),
                        ("Commit Message".to_string(), env!(concat!("VERGEN_GIT_", $prefix, "_COMMIT_MESSAGE")).trim().to_string()),
                        ("Commit Count".to_string(), env!(concat!("VERGEN_GIT_", $prefix, "_COMMIT_COUNT")).trim().to_string()),
                        ("Describe".to_string(), env!(concat!("VERGEN_GIT_", $prefix, "_DESCRIBE")).trim().to_string()),
                        ("Dirty".to_string(), env!(concat!("VERGEN_GIT_", $prefix, "_DIRTY")).trim().to_string()),
                    ],
                ));
            }
        )*
    };
}

pub fn show_build_metadata(version: &str) {
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

    let total_memory = env!("VERGEN_SYSINFO_TOTAL_MEMORY");
    let cpu_frequency = format!("{} MHz", env!("VERGEN_SYSINFO_CPU_FREQUENCY"));

    let build_timestamp_utc = env!("VERGEN_BUILD_TIMESTAMP");
    let build_timezone_name = env!("VERGEN_BUILD_TIMEZONE_NAME");
    let build_timezone_offset = env!("VERGEN_BUILD_TIMEZONE_OFFSET");
    let build_timezone = format!(
        "{} ({})",
        build_timezone_name.trim(),
        build_timezone_offset.trim()
    );

    let (build_timestamp_local, build_date_local) =
        if let Ok(dt_utc) = chrono::DateTime::parse_from_rfc3339(build_timestamp_utc) {
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

    let header_line_about = "About";
    let header_line_version = format!("Tabsdata Version {}", version);

    let mut sections = vec![(
        "Build Information",
        vec![
            ("Build Date (UTC)", env!("VERGEN_BUILD_DATE").trim()),
            (
                "Build Timestamp (UTC)",
                env!("VERGEN_BUILD_TIMESTAMP").trim(),
            ),
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

    sections.extend(vec![
        (
            "Rust Information",
            vec![
                ("Rust Version", env!("VERGEN_RUSTC_SEMVER").trim()),
                ("Rust Channel", env!("VERGEN_RUSTC_CHANNEL").trim()),
                ("Rust Host Triple", env!("VERGEN_RUSTC_HOST_TRIPLE").trim()),
                ("Rust Commit Hash", env!("VERGEN_RUSTC_COMMIT_HASH").trim()),
                ("Rust Commit Date", env!("VERGEN_RUSTC_COMMIT_DATE").trim()),
                ("LLVM Version", env!("VERGEN_RUSTC_LLVM_VERSION").trim()),
            ],
        ),
        (
            "Cargo Information",
            vec![
                ("Target Triple", env!("VERGEN_CARGO_TARGET_TRIPLE").trim()),
                ("Features", env!("VERGEN_CARGO_FEATURES").trim()),
                ("Debug", env!("VERGEN_CARGO_DEBUG").trim()),
                ("Optimization Level", env!("VERGEN_CARGO_OPT_LEVEL").trim()),
            ],
        ),
        (
            "Build System Information",
            vec![
                ("Host", env!("VERGEN_SYSINFO_HOST").trim()),
                ("User", env!("VERGEN_SYSINFO_USER").trim()),
                ("OS Name", env!("VERGEN_SYSINFO_NAME").trim()),
                ("OS Version", env!("VERGEN_SYSINFO_OS_VERSION").trim()),
                ("CPU Vendor", env!("VERGEN_SYSINFO_CPU_VENDOR").trim()),
                ("CPU Brand", env!("VERGEN_SYSINFO_CPU_BRAND").trim()),
                ("CPU Name", env!("VERGEN_SYSINFO_CPU_NAME").trim()),
                (
                    "CPU Core Count",
                    env!("VERGEN_SYSINFO_CPU_CORE_COUNT").trim(),
                ),
                ("CPU Frequency", cpu_frequency.trim()),
                ("Total Memory", total_memory.trim()),
            ],
        ),
    ]);

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
