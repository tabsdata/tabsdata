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
const POD_NAMESPACE: &str = "default";
const POD_UID: &str = "d3744bc5-73c9-4f49-ae29-c9c7855b1c16";
const COMPACT_POD_UID: &str = "d3744bc573";
const TEMPLATE_HASH: &str = "-bbbbbbbbbb";
const CRON_MINUTE: &str = "-99999999";
const RANDOM_SUFFIX: &str = "-bbbbb";
const LONGEST_WORKER_ID: &str = "wkr99991231-9999999999";
const CONTAINER_ROOT: &str = "$(CONTAINER_ROOT)/";
const WINDOWS_PLACEHOLDERS: [(&str, &str); 2] = [
    ("${{system:tdk_pyenv_scripts_folder}}", "Scripts"),
    ("${{system:tdk_exe_suffix}}", ".exe"),
];

fn runtime_workload_names() -> std::collections::HashMap<String, String> {
    let worker_id = WorkerId::try_from(LONGEST_WORKER_ID).unwrap();
    let functions = [
        Decorator::Publisher,
        Decorator::Transformer,
        Decorator::Subscriber,
    ]
    .map(|decorator| (WorkerName::Function, decorator));
    let triggers = [WorkerName::StageTrigger, WorkerName::CronTrigger]
        .map(|worker| (worker, Decorator::Publisher));
    functions
        .into_iter()
        .chain(triggers)
        .map(|(worker, decorator)| {
            let key = format!("{}-{}", worker_pod_stem(&worker), decorator.as_ref());
            let decorator = matches!(worker, WorkerName::Function).then_some(&decorator);
            let prefix = worker_name_prefix(&worker, decorator);
            (key, to_fn_pod_name(prefix, &worker_id).unwrap())
        })
        .collect()
}

fn workload_pods<'a>(
    document: &'a serde_yaml::Value,
    name: &str,
) -> Option<(String, String, &'a serde_yaml::Value)> {
    let spec = &document["spec"];
    match document["kind"].as_str()? {
        "Deployment" => Some((
            format!("{name}{TEMPLATE_HASH}{RANDOM_SUFFIX}"),
            name.to_owned(),
            &spec["template"]["spec"],
        )),
        "Job" => Some((
            format!("{name}{RANDOM_SUFFIX}"),
            name.to_owned(),
            &spec["template"]["spec"],
        )),
        "CronJob" => Some((
            format!("{name}{CRON_MINUTE}{RANDOM_SUFFIX}"),
            format!("{name}{CRON_MINUTE}"),
            &spec["jobTemplate"]["spec"]["template"]["spec"],
        )),
        "Pod" => Some((name.to_owned(), name.to_owned(), spec)),
        _ => None,
    }
}

#[test]
fn every_kminus_program_path_fits_windows_for_users_and_for_ga() {
    let folder = std::path::PathBuf::from(tm_workspace::workspace_root!())
        .join("variant/resources/kprofile/pods");
    let runtime_names = runtime_workload_names();
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
        let template = file_name.split('_').next().unwrap();
        let yaml = std::fs::read_to_string(file).unwrap();
        for document in serde_yaml::Deserializer::from_str(&yaml) {
            let document = serde_yaml::Value::deserialize(document)
                .unwrap_or_else(|error| panic!("{file_name} is YAML: {error}"));
            let Some(name) = runtime_names
                .get(template)
                .cloned()
                .or_else(|| document["metadata"]["name"].as_str().map(str::to_owned))
            else {
                continue;
            };
            let Some((pod, base, pod_spec)) = workload_pods(&document, &name) else {
                continue;
            };
            let dirs = [
                ("full", format!("{POD_NAMESPACE}_{pod}_{POD_UID}")),
                (
                    "compact",
                    format!("{POD_NAMESPACE}_{base}_{COMPACT_POD_UID}"),
                ),
            ];
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
                for (mode, dir) in &dirs {
                    let path = format!(r"{pod_run}\{dir}\work\{container_name}\{program}");
                    checked += 1;
                    if path.len() > WINDOWS_PATH_LIMIT {
                        too_long.push(format!(
                            "{file_name}, container '{container_name}', {mode} pod dirs: \
                             '{command}' is {path} ({} characters)",
                            path.len()
                        ));
                    }
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
