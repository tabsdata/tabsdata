//
// Copyright 2026 Tabsdata Inc.
//

use serial_test::serial;

use super::*;

const UV_ENVIRONMENT_VARIABLES: [&str; 4] = [
    ENV_TD_KUBE_UV_MODE,
    ENV_UV_DEFAULT_INDEX,
    ENV_UV_INDEX,
    ENV_UV_INDEX_STRATEGY,
];

fn with_environment<T>(entries: &[(&str, &str)], assertion: impl FnOnce() -> T) -> T {
    for variable in UV_ENVIRONMENT_VARIABLES {
        unsafe { std::env::remove_var(variable) };
    }
    for (variable, value) in entries {
        unsafe { std::env::set_var(variable, value) };
    }
    let outcome = assertion();
    for variable in UV_ENVIRONMENT_VARIABLES {
        unsafe { std::env::remove_var(variable) };
    }
    outcome
}

#[test]
#[serial]
fn named_modes_are_recognized() {
    for (value, expected) in [
        (UV_MODE_LOCAL, UvMode::Local),
        (UV_MODE_DEVPI, UvMode::Devpi),
        (UV_MODE_PYPI, UvMode::Pypi),
    ] {
        let mode = with_environment(&[(ENV_TD_KUBE_UV_MODE, value)], || {
            UvMode::from_environment()
        });
        assert_eq!(mode.unwrap(), expected);
    }
}

#[test]
#[serial]
fn mode_is_case_insensitive_and_trimmed() {
    let mode = with_environment(&[(ENV_TD_KUBE_UV_MODE, "  DevPi  ")], || {
        UvMode::from_environment()
    });
    assert_eq!(mode.unwrap(), UvMode::Devpi);
}

#[test]
#[serial]
fn missing_or_empty_mode_is_unset() {
    let missing = with_environment(&[], UvMode::from_environment);
    assert_eq!(missing.unwrap(), UvMode::Unset);

    let empty = with_environment(&[(ENV_TD_KUBE_UV_MODE, "   ")], || {
        UvMode::from_environment()
    });
    assert_eq!(empty.unwrap(), UvMode::Unset);
}

#[test]
#[serial]
fn unsupported_mode_fails() {
    let outcome = with_environment(&[(ENV_TD_KUBE_UV_MODE, "devpy")], || {
        UvMode::from_environment()
    });
    let error = outcome.expect_err("An unsupported mode must be rejected.");
    let message = error.to_string();
    assert!(message.contains("devpy"));
    assert!(message.contains(UV_MODE_DEVPI));
}

#[test]
#[serial]
fn named_modes_ignore_the_index_environment_variables() {
    let entries = [
        (ENV_UV_DEFAULT_INDEX, "http://localhost:3142/default/"),
        (ENV_UV_INDEX, "http://localhost:3142/extra/"),
        (ENV_UV_INDEX_STRATEGY, "first-index"),
    ];
    for (mode, expected_default, expected_extra, expected_strategy) in [
        (UvMode::Local, "", "", UV_INDEX_STRATEGY_BEST_MATCH),
        (
            UvMode::Devpi,
            PYPI_INDEX_URL,
            DEVPI_INDEX_URL_HOST,
            UV_INDEX_STRATEGY_BEST_MATCH,
        ),
        (
            UvMode::Pypi,
            PYPI_INDEX_URL,
            "",
            UV_INDEX_STRATEGY_BEST_MATCH,
        ),
    ] {
        with_environment(&entries, || {
            assert_eq!(mode.uv_default_index(IndexReach::Host), expected_default);
            assert_eq!(mode.uv_index(IndexReach::Host), expected_extra);
            assert_eq!(mode.uv_index_strategy(), expected_strategy);
        });
    }
}

#[test]
#[serial]
fn devpi_rewrites_the_host_for_containers() {
    assert_eq!(
        UvMode::Devpi.uv_index(IndexReach::Host),
        DEVPI_INDEX_URL_HOST
    );
    assert_eq!(
        UvMode::Devpi.uv_index(IndexReach::HostContainer),
        DEVPI_INDEX_URL_CONTAINER
    );
}

#[test]
#[serial]
fn unset_mode_honors_the_index_environment_variables() {
    with_environment(
        &[
            (ENV_UV_DEFAULT_INDEX, "http://localhost:3142/default/"),
            (ENV_UV_INDEX, "http://localhost:3142/extra/"),
            (ENV_UV_INDEX_STRATEGY, UV_INDEX_STRATEGY_FIRST_INDEX),
        ],
        || {
            assert_eq!(
                UvMode::Unset.uv_default_index(IndexReach::Host),
                "http://localhost:3142/default/"
            );
            assert_eq!(
                UvMode::Unset.uv_index(IndexReach::Host),
                "http://localhost:3142/extra/"
            );
            assert_eq!(
                UvMode::Unset.uv_index_strategy(),
                UV_INDEX_STRATEGY_BEST_MATCH
            );
        },
    );
}

#[test]
#[serial]
fn unset_mode_falls_back_to_pypi_when_no_variable_is_set() {
    with_environment(&[], || {
        assert_eq!(
            UvMode::Unset.uv_default_index(IndexReach::Host),
            PYPI_INDEX_URL
        );
        assert_eq!(UvMode::Unset.uv_index(IndexReach::Host), "");
        assert_eq!(
            UvMode::Unset.uv_index_strategy(),
            UV_INDEX_STRATEGY_BEST_MATCH
        );
    });
}

#[test]
#[serial]
fn unset_mode_always_uses_best_match() {
    with_environment(&[(ENV_UV_INDEX, "http://localhost:3142/extra/")], || {
        assert_eq!(
            UvMode::Unset.uv_index_strategy(),
            UV_INDEX_STRATEGY_BEST_MATCH
        );
    });
}

#[test]
#[serial]
fn unset_mode_treats_blank_variables_as_absent() {
    with_environment(
        &[
            (ENV_UV_DEFAULT_INDEX, "   "),
            (ENV_UV_INDEX, "   "),
            (ENV_UV_INDEX_STRATEGY, "   "),
        ],
        || {
            assert_eq!(
                UvMode::Unset.uv_default_index(IndexReach::Host),
                PYPI_INDEX_URL
            );
            assert_eq!(UvMode::Unset.uv_index(IndexReach::Host), "");
            assert_eq!(
                UvMode::Unset.uv_index_strategy(),
                UV_INDEX_STRATEGY_BEST_MATCH
            );
        },
    );
}

#[test]
#[serial]
fn unset_mode_trims_variable_values() {
    with_environment(
        &[(ENV_UV_INDEX, "  http://localhost:3142/extra/  ")],
        || {
            assert_eq!(
                UvMode::Unset.uv_index(IndexReach::Host),
                "http://localhost:3142/extra/"
            );
        },
    );
}

#[test]
#[serial]
fn unset_mode_rewrites_a_loopback_index_for_containers() {
    for host in ["localhost", "127.0.0.1", "[::1]"] {
        let index = format!("http://{host}:3142/tabsdata/td/+simple/");
        let rewritten = with_environment(&[(ENV_UV_INDEX, index.as_str())], || {
            UvMode::Unset.uv_index(IndexReach::HostContainer)
        });
        assert_eq!(
            rewritten,
            format!("http://{CONTAINER_HOST_GATEWAY}:3142/tabsdata/td/+simple/"),
            "host {host}"
        );
    }
}

#[test]
#[serial]
fn unset_mode_keeps_a_loopback_index_for_the_host() {
    let index = "http://localhost:3142/tabsdata/td/+simple/";
    let kept = with_environment(&[(ENV_UV_INDEX, index)], || {
        UvMode::Unset.uv_index(IndexReach::Host)
    });
    assert_eq!(kept, index);
}

#[test]
#[serial]
fn unset_mode_keeps_a_routable_index_for_containers() {
    let index = "https://devpi.example.com:3142/tabsdata/td/+simple/";
    let kept = with_environment(&[(ENV_UV_INDEX, index)], || {
        UvMode::Unset.uv_index(IndexReach::HostContainer)
    });
    assert_eq!(kept, index);
}

#[test]
#[serial]
fn cluster_reach_always_uses_the_in_cluster_index() {
    let entries = [(ENV_UV_INDEX, "http://localhost:3142/tabsdata/td/+simple/")];
    let resolved = with_environment(&entries, || UvMode::Devpi.uv_index(IndexReach::Cluster));
    assert_eq!(resolved, DEVPI_INDEX_URL_CLUSTER);
}

#[test]
#[serial]
fn cluster_reach_stays_empty_for_local_and_pypi() {
    for mode in [UvMode::Local, UvMode::Pypi] {
        let resolved = with_environment(&[], || mode.uv_index(IndexReach::Cluster));
        assert_eq!(resolved, "", "mode {mode:?}");
    }
}

#[test]
#[serial]
fn devpi_serves_tabsdata_and_pypi_serves_the_rest() {
    with_environment(&[], || {
        assert_eq!(
            UvMode::Devpi.uv_default_index(IndexReach::Host),
            PYPI_INDEX_URL
        );
        assert_eq!(
            UvMode::Devpi.uv_index(IndexReach::Cluster),
            DEVPI_INDEX_URL_CLUSTER
        );
        assert_eq!(
            UvMode::Devpi.uv_index_strategy(),
            UV_INDEX_STRATEGY_BEST_MATCH
        );
    });
}

#[test]
#[serial]
fn cluster_reach_ignores_the_local_index_environment_variables() {
    with_environment(
        &[
            (ENV_UV_DEFAULT_INDEX, "http://localhost:3142/default/"),
            (ENV_UV_INDEX, "http://localhost:3142/extra/"),
            (ENV_UV_INDEX_STRATEGY, UV_INDEX_STRATEGY_FIRST_INDEX),
        ],
        || {
            assert_eq!(
                UvMode::Unset.uv_default_index(IndexReach::Cluster),
                PYPI_INDEX_URL
            );
            assert_eq!(UvMode::Unset.uv_index(IndexReach::Cluster), "");
            assert_eq!(
                UvMode::Unset.uv_index_strategy(),
                UV_INDEX_STRATEGY_BEST_MATCH
            );
        },
    );
}

#[test]
#[serial]
fn devpi_mode_still_reaches_the_cluster_index() {
    assert_eq!(
        UvMode::Devpi.uv_index(IndexReach::Cluster),
        DEVPI_INDEX_URL_CLUSTER
    );
}

#[test]
#[serial]
fn only_local_reaches_propagate_local_settings() {
    assert!(IndexReach::Host.propagates_local_settings());
    assert!(IndexReach::HostContainer.propagates_local_settings());
    assert!(!IndexReach::Cluster.propagates_local_settings());
}

#[test]
fn every_venv_maps_to_its_requirements_asset() {
    assert_eq!(Venv::Td.requirements_asset(), REQUIREMENTS_TD_ASSET);
    assert_eq!(Venv::Fn.requirements_asset(), REQUIREMENTS_FN_ASSET);
    #[cfg(feature = "enterprise")]
    assert_eq!(Venv::Ag.requirements_asset(), REQUIREMENTS_AG_ASSET);
}

#[test]
fn every_venv_requirements_asset_is_customizable() {
    for venv in VENVS {
        assert!(
            CUSTOMIZABLE_ASSETS.contains(&venv.requirements_asset()),
            "{} is not customizable",
            venv.as_str()
        );
    }
}

#[test]
fn the_listed_venvs_are_unique_and_described() {
    let mut names: Vec<&str> = VENVS.iter().map(|venv| venv.as_str()).collect();
    let listed = names.len();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), listed, "the venv names repeat");
    for venv in VENVS {
        assert!(
            !venv.description().is_empty(),
            "{} has no description",
            venv.as_str()
        );
    }
}

#[test]
fn the_open_source_venvs_are_always_listed() {
    let names: Vec<&str> = VENVS.iter().map(|venv| venv.as_str()).collect();
    assert!(names.contains(&VENV_TD));
    assert!(names.contains(&VENV_FN));
}

#[cfg(feature = "enterprise")]
#[test]
fn the_agent_venv_is_listed_in_the_enterprise_edition() {
    let names: Vec<&str> = VENVS.iter().map(|venv| venv.as_str()).collect();
    assert!(names.contains(&VENV_AG));
}
