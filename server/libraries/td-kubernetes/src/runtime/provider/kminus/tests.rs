//
// Copyright 2026 Tabsdata Inc.
//

use super::*;
use crate::base::constants::{REQUIREMENTS_FN_ASSET, REQUIREMENTS_TD_ASSET};
use crate::runtime::provider::kminus::executable::WINDOWS_PATH_LIMIT;

#[test]
fn write_requirements_writes_one_file_per_venv_asset() {
    let folder = tempfile::tempdir().unwrap();
    let assets = RenderedConfigmap::from([
        (REQUIREMENTS_TD_ASSET.to_owned(), "# none\n".to_owned()),
        (REQUIREMENTS_FN_ASSET.to_owned(), "polars\n".to_owned()),
        ("venvbuilder.py".to_owned(), "print()\n".to_owned()),
    ]);

    let requirements = write_requirements(folder.path(), &assets).unwrap();

    assert_eq!(
        requirements,
        vec![
            (Venv::Td, folder.path().join(REQUIREMENTS_TD_ASSET)),
            (Venv::Fn, folder.path().join(REQUIREMENTS_FN_ASSET)),
        ]
    );
    let fn_requirements = fs::read_to_string(folder.path().join(REQUIREMENTS_FN_ASSET)).unwrap();
    assert_eq!(fn_requirements, "polars\n");
}

#[test]
fn write_requirements_skips_venvs_without_an_asset() {
    let folder = tempfile::tempdir().unwrap();
    let assets =
        RenderedConfigmap::from([(REQUIREMENTS_FN_ASSET.to_owned(), "polars\n".to_owned())]);

    let requirements = write_requirements(folder.path(), &assets).unwrap();

    assert_eq!(
        requirements,
        vec![(Venv::Fn, folder.path().join(REQUIREMENTS_FN_ASSET))]
    );
}

#[test]
fn executable_paths_are_enforced_only_on_windows() {
    let provider = KminusProvider::build(
        ClusterArguments::new(Some(ClusterProvider::Kminus), None, None, false, None),
        InstanceArguments::new(Some("tabsdata".to_owned())),
        false,
        None,
        None,
    )
    .unwrap();
    let executable = "x".repeat(WINDOWS_PATH_LIMIT);
    let pods = RenderedConfigmap::from([(
        "apiserver.yaml".to_owned(),
        format!(
            "kind: Pod\nspec:\n  containers:\n    - name: main\n      \
             command: [\"$(CONTAINER_ROOT)/{executable}\"]\n"
        ),
    )]);

    let outcome = provider.ensure_executable_paths_fit(&pods, &SystemValues::default());

    if cfg!(windows) {
        let error = outcome.unwrap_err();
        let message = error.source().unwrap().to_string();
        assert!(message.contains(&executable), "{message}");
        assert!(
            message.contains(&*provider.cluster.data_path().to_string_lossy()),
            "{message}"
        );
    } else {
        outcome.unwrap();
    }
}
