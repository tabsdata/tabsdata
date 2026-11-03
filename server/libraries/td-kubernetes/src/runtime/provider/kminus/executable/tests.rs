//
// Copyright 2026 Tabsdata Inc.
//

use super::*;
use crate::runtime::instance::profile::load_profile_from;
use crate::runtime::provider::ClusterProvider;

const USER_NAME_MAX: usize = 12;
const INSTANCE_PREFIX_MAX: usize = 20;
const INSTANCE_STAMP_SUFFIX: &str = "-20261030060741515";

fn windows_system() -> SystemValues {
    SystemValues {
        pyenv_scripts_folder: Some("Scripts".to_owned()),
        exe_suffix: Some(".exe".to_owned()),
        ..SystemValues::default()
    }
}

fn pod(kind: &str, containers: &[(&str, &str)]) -> String {
    let containers: String = containers
        .iter()
        .map(|(name, command)| format!("    - name: {name}\n      command: [\"{command}\"]\n"))
        .collect();
    format!("kind: {kind}\nspec:\n  containers:\n{containers}")
}

fn pods(entries: &[(&str, String)]) -> RenderedConfigmap {
    entries
        .iter()
        .map(|(name, yaml)| (name.to_string(), yaml.clone()))
        .collect()
}

fn data_folder_for(path_length: usize, container: &str, program: &str) -> String {
    let fixed = [
        POD_RUN_FOLDER,
        POD_UID_PLACEHOLDER,
        CONTAINER_WORK_FOLDER,
        container,
        program,
    ]
    .iter()
    .map(|segment| segment.len() + 1)
    .sum::<usize>();
    format!(r"C:\{}", "d".repeat(path_length - fixed - 3))
}

#[test]
fn executable_path_is_built_under_the_pod_run_folder_of_the_data_folder() {
    let pods = pods(&[(
        "apiserver.yaml",
        pod(
            "Pod",
            &[(
                "apiserver",
                "$(CONTAINER_ROOT)/tabsdata/pyenv/${{system:tdk_pyenv_scripts_folder}}/td${{system:tdk_exe_suffix}}",
            )],
        ),
    )]);
    let paths = windows_executable_paths(r"C:\data", &pods, &windows_system()).unwrap();
    assert_eq!(paths.len(), 1);
    assert_eq!(
        paths[0].path,
        format!(
            r"C:\data\pod-run\{POD_UID_PLACEHOLDER}\work\apiserver\tabsdata\pyenv\Scripts\td.exe"
        )
    );
    assert_eq!(POD_UID_PLACEHOLDER.len(), 36);
}

#[test]
fn executables_outside_the_container_root_are_not_paths_of_the_instance() {
    let pods = pods(&[("probe.yaml", pod("Pod", &[("probe", "/usr/bin/probe")]))]);
    let paths = windows_executable_paths(r"C:\data", &pods, &windows_system()).unwrap();
    assert!(paths.is_empty());
}

#[test]
fn unresolved_executable_placeholder_is_rejected() {
    let pods = pods(&[(
        "probe.yaml",
        pod(
            "Pod",
            &[("probe", "$(CONTAINER_ROOT)/${{system:tdk_runtimes_root}}/x")],
        ),
    )]);
    let error = windows_executable_paths(r"C:\data", &pods, &windows_system()).unwrap_err();
    assert!(matches!(
        error,
        ProviderError::ExecutablePlaceholderUnresolved { .. }
    ));
}

#[test]
fn path_at_the_limit_fits() {
    let program = "$(CONTAINER_ROOT)/td.exe";
    let pods = pods(&[("a.yaml", pod("Pod", &[("main", program)]))]);
    let data = data_folder_for(WINDOWS_PATH_LIMIT, "main", "td.exe");
    let paths = windows_executable_paths(&data, &pods, &windows_system()).unwrap();
    assert_eq!(paths[0].length(), WINDOWS_PATH_LIMIT);
    ensure_windows_executable_paths_fit("tabsdata", &data, &pods, &windows_system()).unwrap();
}

#[test]
fn path_one_character_over_the_limit_is_rejected() {
    let program = "$(CONTAINER_ROOT)/td.exe";
    let pods = pods(&[("a.yaml", pod("Pod", &[("main", program)]))]);
    let data = data_folder_for(WINDOWS_PATH_LIMIT + 1, "main", "td.exe");
    let error = ensure_windows_executable_paths_fit("tabsdata", &data, &pods, &windows_system())
        .unwrap_err();
    let ProviderError::ExecutablePathTooLong {
        instance,
        length,
        limit,
        ..
    } = &error
    else {
        panic!("unexpected error: {error}");
    };
    assert_eq!(instance, "tabsdata");
    assert_eq!(*length, WINDOWS_PATH_LIMIT + 1);
    assert_eq!(*limit, WINDOWS_PATH_LIMIT);
    let message = error.to_string();
    assert!(message.contains("home folder"), "{message}");
    assert!(message.contains("instance name"), "{message}");
    assert!(message.contains("headroom"), "{message}");
}

#[test]
fn longest_offending_path_is_reported() {
    let pods = pods(&[
        (
            "a.yaml",
            "kind: Deployment\n\
             spec:\n  \
               template:\n    \
                 spec:\n      \
                   initContainers:\n        \
                     - name: main\n          \
                       command: [\"$(CONTAINER_ROOT)/longer-program.exe\"]\n"
                .to_owned(),
        ),
        (
            "b.yaml",
            pod("Pod", &[("main", "$(CONTAINER_ROOT)/short.exe")]),
        ),
    ]);
    let data = data_folder_for(WINDOWS_PATH_LIMIT + 2, "main", "short.exe");
    let error = ensure_windows_executable_paths_fit("tabsdata", &data, &pods, &windows_system())
        .unwrap_err();
    let ProviderError::ExecutablePathTooLong { path, length, .. } = error else {
        panic!("unexpected error: {error}");
    };
    assert!(path.ends_with(r"\main\longer-program.exe"), "{path}");
    assert_eq!(length, path.len());
    assert!(length > WINDOWS_PATH_LIMIT + 2);
}

#[test]
fn every_kminus_executable_path_fits_windows() {
    let root = std::path::PathBuf::from(tm_workspace::workspace_root!())
        .join("variant/resources/kprofile");
    let profile = load_profile_from(&root, ClusterProvider::Kminus, "0.0.0")
        .unwrap_or_else(|error| panic!("{} loads: {error}", root.display()));
    let user = "u".repeat(USER_NAME_MAX);
    let instance = format!("{}{INSTANCE_STAMP_SUFFIX}", "i".repeat(INSTANCE_PREFIX_MAX));
    let data_folder = format!(r"C:\Users\{user}\.tabsdata\instances\{instance}\cluster\data");

    let paths = windows_executable_paths(&data_folder, &profile.pods, &windows_system())
        .unwrap_or_else(|error| panic!("{error}"));
    assert!(
        !paths.is_empty(),
        "no executable path found under {}",
        root.display()
    );

    let too_long: Vec<_> = paths
        .iter()
        .filter(|path| path.length() > WINDOWS_PATH_LIMIT)
        .map(|path| {
            format!(
                "{}, container '{}': '{}' is {} ({} characters)",
                path.pod,
                path.container,
                path.command,
                path.path,
                path.length()
            )
        })
        .collect();
    assert!(
        too_long.is_empty(),
        "{} executable path(s) exceed Windows' {WINDOWS_PATH_LIMIT} characters, for a user \
         name of up to {USER_NAME_MAX} characters and an instance name of up to \
         {INSTANCE_PREFIX_MAX} characters plus its '{INSTANCE_STAMP_SUFFIX}'-like suffix:\n  {}",
        too_long.len(),
        too_long.join("\n  ")
    );
}
