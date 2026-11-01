//
// Copyright 2026 Tabsdata Inc.
//

use serde::Deserialize;

use super::*;

#[test]
fn to_fn_pod_name_spells_the_worker_id_as_the_api_does() {
    let worker_id = WorkerId::try_from("wkr20260713-0000000005").unwrap();
    let name = to_fn_pod_name("pub", &worker_id).unwrap();
    assert_eq!(name, "pub-wkr20260713-5");
    assert!(DNS_SUBDOMAIN_PATTERN.is_match(&name), "{name}");
}

#[test]
fn logical_id_text_is_the_unpadded_spelling() {
    let worker_id = WorkerId::try_from("wkr20260713-0000000005").unwrap();
    assert_eq!(logical_id_text(&worker_id), "wkr20260713-5");
}

#[test]
fn normalize_segment_keeps_the_shape_of_the_value() {
    let normalized = normalize_segment(NameSegment::WorkerId, "WKR20260713.5").unwrap();
    assert_eq!(normalized, "wkr20260713-5");
    let normalized = normalize_segment(NameSegment::WorkerId, "-a__b-").unwrap();
    assert_eq!(normalized, "a-b");
}

#[test]
fn normalize_segment_rejects_all_separators() {
    assert!(normalize_segment(NameSegment::WorkerId, "...").is_err());
}

#[test]
fn worker_name_prefix_maps_each_worker_and_decorator() {
    assert_eq!(
        worker_name_prefix(&WorkerName::Function, Some(&Decorator::Publisher)),
        "pub"
    );
    assert_eq!(
        worker_name_prefix(&WorkerName::Function, Some(&Decorator::Subscriber)),
        "sub"
    );
    assert_eq!(
        worker_name_prefix(&WorkerName::Function, Some(&Decorator::Transformer)),
        "tfr"
    );
    assert_eq!(worker_name_prefix(&WorkerName::Function, None), "fun");
    assert_eq!(worker_name_prefix(&WorkerName::CronTrigger, None), "crn");
    assert_eq!(worker_name_prefix(&WorkerName::StageTrigger, None), "stm");
}

const WINDOWS_PATH_LIMIT: usize = 259;
const USER_NAME_MAX: usize = 12;
const INSTANCE_PREFIX_MAX: usize = 20;
const INSTANCE_STAMP_SUFFIX: &str = "-20261030060741515";
const POD_UID: &str = "d3744bc5-73c9-4f49-ae29-c9c7855b1c16";
const CONTAINER_ROOT: &str = "$(CONTAINER_ROOT)/";
const WINDOWS_PLACEHOLDERS: [(&str, &str); 2] = [
    ("${{system:tdk_pyenv_scripts_folder}}", "Scripts"),
    ("${{system:tdk_exe_suffix}}", ".exe"),
];

fn workload_pod_spec(document: &serde_yaml::Value) -> Option<&serde_yaml::Value> {
    let spec = &document["spec"];
    match document["kind"].as_str()? {
        "Deployment" | "Job" => Some(&spec["template"]["spec"]),
        "CronJob" => Some(&spec["jobTemplate"]["spec"]["template"]["spec"]),
        "Pod" => Some(spec),
        _ => None,
    }
}

// The pod-runner names a pod's data dir by its uid alone, so a pod's name
// plays no part in the length of the paths under it
#[test]
fn every_kminus_program_path_fits_windows() {
    let folder = std::path::PathBuf::from(tm_workspace::workspace_root!())
        .join("variant/resources/kprofile/pods");
    let user = "u".repeat(USER_NAME_MAX);
    let instance = format!("{}{INSTANCE_STAMP_SUFFIX}", "i".repeat(INSTANCE_PREFIX_MAX));
    let pod_run = format!(r"C:\Users\{user}\.tabsdata\instances\{instance}\cluster\data\pod-run");

    let mut files: Vec<_> = std::fs::read_dir(&folder)
        .unwrap_or_else(|error| panic!("{} is readable: {error}", folder.display()))
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "yaml")
        })
        .collect();
    files.sort();

    let mut checked = 0;
    let mut too_long = Vec::new();
    for file in &files {
        let file_name = file.file_name().unwrap().to_string_lossy().into_owned();
        let yaml = std::fs::read_to_string(file).unwrap();
        for document in serde_yaml::Deserializer::from_str(&yaml) {
            let document = serde_yaml::Value::deserialize(document)
                .unwrap_or_else(|error| panic!("{file_name} is YAML: {error}"));
            let Some(pod_spec) = workload_pod_spec(&document) else {
                continue;
            };
            let containers = ["initContainers", "containers"]
                .into_iter()
                .filter_map(|key| pod_spec[key].as_sequence())
                .flatten();
            for container in containers {
                let Some(command) = container["command"][0].as_str() else {
                    continue;
                };
                let Some(program) = command.strip_prefix(CONTAINER_ROOT) else {
                    continue;
                };
                let program = WINDOWS_PLACEHOLDERS
                    .iter()
                    .fold(program.to_owned(), |program, (placeholder, value)| {
                        program.replace(placeholder, value)
                    })
                    .replace('/', r"\");
                assert!(
                    !program.contains("${{"),
                    "{file_name}: '{command}' has a placeholder this test does not resolve"
                );
                let container_name = container["name"].as_str().unwrap();
                let path = format!(r"{pod_run}\{POD_UID}\work\{container_name}\{program}");
                checked += 1;
                if path.len() > WINDOWS_PATH_LIMIT {
                    too_long.push(format!(
                        "{file_name}, container '{container_name}': '{command}' is {path} ({} \
                         characters)",
                        path.len()
                    ));
                }
            }
        }
    }

    assert!(
        checked > 0,
        "no program path found under {}",
        folder.display()
    );
    assert!(
        too_long.is_empty(),
        "{} program path(s) exceed Windows' {WINDOWS_PATH_LIMIT} characters, for a user \
         name of up to {USER_NAME_MAX} characters and an instance name of up to \
         {INSTANCE_PREFIX_MAX} characters plus its '{INSTANCE_STAMP_SUFFIX}'-like suffix:\n  {}",
        too_long.len(),
        too_long.join("\n  ")
    );
}
