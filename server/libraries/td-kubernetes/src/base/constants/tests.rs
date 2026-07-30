//
// Copyright 2026 Tabsdata Inc.
//

use serial_test::serial;

use super::*;

const UV_ENVIRONMENT_VARIABLES: [&str; 4] = [
    ENV_TD_KUBE_UV_MODE,
    ENV_UV_DEFAULT_INDEX,
    ENV_UV_EXTRA_INDEX_URL,
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
        (ENV_UV_EXTRA_INDEX_URL, "http://localhost:3142/extra/"),
        (ENV_UV_INDEX_STRATEGY, "first-index"),
    ];
    for (mode, expected_extra, expected_strategy) in [
        (UvMode::Local, "", UV_INDEX_STRATEGY_FIRST_INDEX),
        (
            UvMode::Devpi,
            DEVPI_INDEX_URL_HOST,
            UV_INDEX_STRATEGY_BEST_MATCH,
        ),
        (UvMode::Pypi, "", UV_INDEX_STRATEGY_FIRST_INDEX),
    ] {
        with_environment(&entries, || {
            assert_eq!(mode.default_index(), "");
            assert_eq!(mode.extra_index_url(false), expected_extra);
            assert_eq!(mode.index_strategy(), expected_strategy);
        });
    }
}

#[test]
#[serial]
fn devpi_rewrites_the_host_for_containers() {
    assert_eq!(UvMode::Devpi.extra_index_url(false), DEVPI_INDEX_URL_HOST);
    assert_eq!(
        UvMode::Devpi.extra_index_url(true),
        DEVPI_INDEX_URL_CONTAINER
    );
}

#[test]
#[serial]
fn unset_mode_honors_the_index_environment_variables() {
    with_environment(
        &[
            (ENV_UV_DEFAULT_INDEX, "http://localhost:3142/default/"),
            (ENV_UV_EXTRA_INDEX_URL, "http://localhost:3142/extra/"),
            (ENV_UV_INDEX_STRATEGY, UV_INDEX_STRATEGY_FIRST_INDEX),
        ],
        || {
            assert_eq!(
                UvMode::Unset.default_index(),
                "http://localhost:3142/default/"
            );
            assert_eq!(
                UvMode::Unset.extra_index_url(false),
                "http://localhost:3142/extra/"
            );
            assert_eq!(
                UvMode::Unset.index_strategy(),
                UV_INDEX_STRATEGY_FIRST_INDEX
            );
        },
    );
}

#[test]
#[serial]
fn unset_mode_falls_back_to_pypi_when_no_variable_is_set() {
    with_environment(&[], || {
        assert_eq!(UvMode::Unset.default_index(), PYPI_INDEX_URL);
        assert_eq!(UvMode::Unset.extra_index_url(false), "");
        assert_eq!(UvMode::Unset.index_strategy(), "");
    });
}

#[test]
#[serial]
fn unset_mode_infers_best_match_when_only_an_extra_index_is_set() {
    with_environment(
        &[(ENV_UV_EXTRA_INDEX_URL, "http://localhost:3142/extra/")],
        || {
            assert_eq!(UvMode::Unset.index_strategy(), UV_INDEX_STRATEGY_BEST_MATCH);
        },
    );
}

#[test]
#[serial]
fn unset_mode_treats_blank_variables_as_absent() {
    with_environment(
        &[
            (ENV_UV_DEFAULT_INDEX, "   "),
            (ENV_UV_EXTRA_INDEX_URL, "   "),
            (ENV_UV_INDEX_STRATEGY, "   "),
        ],
        || {
            assert_eq!(UvMode::Unset.default_index(), PYPI_INDEX_URL);
            assert_eq!(UvMode::Unset.extra_index_url(false), "");
            assert_eq!(UvMode::Unset.index_strategy(), "");
        },
    );
}

#[test]
#[serial]
fn unset_mode_trims_variable_values() {
    with_environment(
        &[(ENV_UV_EXTRA_INDEX_URL, "  http://localhost:3142/extra/  ")],
        || {
            assert_eq!(
                UvMode::Unset.extra_index_url(false),
                "http://localhost:3142/extra/"
            );
        },
    );
}
