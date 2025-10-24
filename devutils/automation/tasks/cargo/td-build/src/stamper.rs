//
// Copyright 2025 Tabs Data Inc.
//

use anyhow::Result;
use chrono::Local;
use std::env;
use std::path::PathBuf;
use tm_workspace::workspace_root;
use vergen_gix::{BuildBuilder, CargoBuilder, Emitter, GixBuilder, RustcBuilder, SysinfoBuilder};

const TABSDATA_PROJECT_MARKER: &str = "tabsdata";

const TABSDATA_SOLUTION_HOME: &str = "TABSDATA_SOLUTION_HOME";

const VERGEN_ALREADY_RAN: &str = "TD_VERGEN_ALREADY_RAN";

pub struct GitRepository {
    pub name: &'static str,
    pub description: &'static str,
}

impl GitRepository {
    pub const fn new(name: &'static str, description: &'static str) -> Self {
        Self { name, description }
    }

    pub fn suffix(&self) -> String {
        let prefix_with_dash = format!("{}-", TABSDATA_PROJECT_MARKER);
        self.name
            .strip_prefix(&prefix_with_dash)
            .unwrap_or(self.name)
            .to_string()
    }

    pub fn env_prefix(&self) -> String {
        self.name.to_uppercase().replace('-', "_")
    }
}

macro_rules! define_repositories {
    ($(($name:literal, $desc:literal, $prefix:literal)),* $(,)?) => {
        pub const TABSDATA_REPOSITORIES: &[GitRepository] = &[
            $(GitRepository::new($name, $desc)),*
        ];

        #[macro_export]
        macro_rules! invoke_add_git_sections {
            ($sections:expr, $git_data:expr, $macro_name:ident) => {
                $macro_name!($sections, $git_data, $($prefix),*);
            };
        }
    };
}

define_repositories!(
    ("tabsdata-ee", "Tabsdata Enterprise", "TABSDATA_EE"),
    ("tabsdata-os", "Tabsdata Open Source", "TABSDATA_OS"),
    ("tabsdata-ui", "Tabsdata User Interface", "TABSDATA_UI"),
    ("tabsdata-ag", "Tabsdata Agent", "TABSDATA_AG"),
    ("tabsdata-ci", "Tabsdata Automation", "TABSDATA_CI"),
);

pub struct Stamping;

pub trait Stamper {
    fn stamp() -> Result<()> {
        Ok(())
    }
}

impl Stamping {
    fn solution() -> PathBuf {
        let tabsdata_solution_home = env::var(TABSDATA_SOLUTION_HOME)
            .ok()
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let root = PathBuf::from(workspace_root!());
                root.parent()
                    .expect("Workspace root must have a parent directory")
                    .to_path_buf()
            });
        let path_before_canonicalize = tabsdata_solution_home.clone();
        let tabsdata_solution_home =
            tabsdata_solution_home
                .canonicalize()
                .unwrap_or_else(|error| {
                    panic!(
                        "Failed to canonicalize tabsdata solution home - '{:?}': {}",
                        path_before_canonicalize, error
                    )
                });
        println!(
            "cargo:warning=tabsdata solution home: {:?}",
            tabsdata_solution_home
        );
        match std::fs::read_dir(&tabsdata_solution_home) {
            Ok(entries) => {
                println!("cargo:warning=🪣 Contents of tabsdata solution home:");
                let mut items: Vec<_> = entries.filter_map(|e| e.ok()).collect();
                items.sort_by_key(|e| e.path());
                for entry in items {
                    let path = entry.path();
                    let file_type = if path.is_dir() { "folder" } else { "file" };
                    println!(
                        "cargo:warning=   📚 [{:6}] {}",
                        file_type,
                        path.file_name().unwrap_or_default().to_string_lossy()
                    );
                }
            }
            Err(error) => {
                panic!(
                    "Failed to read contents of tabsdata solution home: {}",
                    error
                );
            }
        }
        tabsdata_solution_home
    }
}

impl Stamper for Stamping {
    fn stamp() -> Result<()> {
        let solution: PathBuf = Self::solution();

        if env::var(VERGEN_ALREADY_RAN).is_ok() {
            println!(
                "cargo:warning=Vergen already ran; skipping to avoid inconsistent metadata generation."
            );
            return Ok(());
        }
        println!("cargo:rustc-env={}=1", VERGEN_ALREADY_RAN);

        let build = BuildBuilder::default()
            .build_date(true)
            .build_timestamp(true)
            .build()?;

        for repository in TABSDATA_REPOSITORIES {
            let repository_path = solution.join(repository.name);
            let repository_prefix = repository.name.to_uppercase().replace('-', "_");
            if !repository_path.exists() {
                println!(
                    "cargo:rustc-env=VERGEN_GIT_{}_EXISTS=false",
                    repository_prefix
                );
                continue;
            }
            println!(
                "cargo:rustc-env=VERGEN_GIT_{}_EXISTS=true",
                repository_prefix
            );
            println!(
                "cargo:rustc-env=VERGEN_GIT_{}_NAME={}",
                repository_prefix, repository.name
            );
            println!(
                "cargo:rustc-env=VERGEN_GIT_{}_DESCRIPTION={}",
                repository_prefix, repository.description
            );
            let gix = GixBuilder::default()
                .repo_path(Some(repository_path.clone()))
                .branch(true)
                .sha(true)
                .commit_date(true)
                .commit_timestamp(true)
                .commit_author_name(true)
                .commit_author_email(true)
                .commit_message(true)
                .commit_count(true)
                .describe(true, false, None)
                .dirty(true)
                .build()?;
            Emitter::default().add_instructions(&gix)?.emit_and_set()?;
            let git_envs = [
                "VERGEN_GIT_BRANCH",
                "VERGEN_GIT_SHA",
                "VERGEN_GIT_COMMIT_DATE",
                "VERGEN_GIT_COMMIT_TIMESTAMP",
                "VERGEN_GIT_COMMIT_AUTHOR_EMAIL",
                "VERGEN_GIT_COMMIT_AUTHOR_NAME",
                "VERGEN_GIT_COMMIT_COUNT",
                "VERGEN_GIT_COMMIT_MESSAGE",
                "VERGEN_GIT_COMMIT_COUNT",
                "VERGEN_GIT_DESCRIBE",
                "VERGEN_GIT_DIRTY",
            ];
            let long_hash: String;
            let tag: String;
            match gix::open(&repository_path) {
                Ok(repo) => {
                    long_hash = repo
                        .head_id()
                        .ok()
                        .map(|id| id.to_hex().to_string())
                        .unwrap_or_else(|| "-".to_string());
                    tag = repo
                        .head_id()
                        .ok()
                        .and_then(|head_id| {
                            repo.references()
                                .ok()?
                                .tags()
                                .ok()?
                                .filter_map(Result::ok)
                                .find(|tag_ref| {
                                    tag_ref
                                        .id()
                                        .object()
                                        .ok()
                                        .and_then(|obj| obj.try_into_commit().ok())
                                        .map(|commit| commit.id == head_id)
                                        .unwrap_or(false)
                                })
                                .and_then(|tag_ref| tag_ref.name().shorten().to_string().into())
                        })
                        .unwrap_or_else(|| "-".to_string());
                }
                Err(_) => {
                    long_hash = "-".to_string();
                    tag = "-".to_string();
                }
            }
            println!(
                "cargo:rustc-env=VERGEN_GIT_{}_LONG_HASH={}",
                repository_prefix, long_hash
            );
            println!(
                "cargo:rustc-env=VERGEN_GIT_{}_TAG={}",
                repository_prefix, tag
            );
            for env_name in &git_envs {
                if let Ok(env_value) = env::var(env_name) {
                    let prefixed_name = env_name
                        .replace("VERGEN_GIT_", &format!("VERGEN_GIT_{}_", repository_prefix));
                    println!("cargo:rustc-env={}={}", prefixed_name, env_value);
                }
            }
            for env_name in &git_envs {
                unsafe {
                    env::remove_var(env_name);
                }
            }
        }
        let rust = RustcBuilder::default()
            .semver(true)
            .channel(true)
            .host_triple(true)
            .commit_hash(true)
            .commit_date(true)
            .llvm_version(true)
            .build()?;

        let cargo = CargoBuilder::default()
            .target_triple(true)
            .features(true)
            .debug(true)
            .opt_level(true)
            .build()?;

        let sysinfo = SysinfoBuilder::default()
            .cpu_brand(true)
            .cpu_core_count(true)
            .cpu_frequency(true)
            .cpu_name(true)
            .cpu_vendor(true)
            .memory(true)
            .name(true)
            .os_version(true)
            .user(true)
            .build()?;
        let hostname = hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "unknown".to_string());
        println!("cargo:rustc-env=VERGEN_SYSINFO_HOST={}", hostname);

        Emitter::default()
            .add_instructions(&build)?
            .add_instructions(&cargo)?
            .add_instructions(&rust)?
            .add_instructions(&sysinfo)?
            .emit()?;

        let now_local = Local::now();
        let timezone_offset = now_local.offset().to_string();
        let timezone_name =
            iana_time_zone::get_timezone().unwrap_or_else(|_| timezone_offset.clone());
        println!(
            "cargo:rustc-env=VERGEN_BUILD_TIMEZONE_OFFSET={}",
            timezone_offset
        );
        println!(
            "cargo:rustc-env=VERGEN_BUILD_TIMEZONE_NAME={}",
            timezone_name.trim()
        );

        Ok(())
    }
}
