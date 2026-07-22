//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

struct Row {
    kind: PlaceholderKind,
    text: &'static str,
    auto: bool,
    editable: bool,
    phase: Phase,
    behavior: Substitution,
    supported: bool,
}

const MATRIX: &[Row] = &[
    Row {
        kind: PlaceholderKind::Literal,
        text: "literal",
        auto: false,
        editable: true,
        phase: Phase::Creation,
        behavior: Substitution::Inlined,
        supported: true,
    },
    Row {
        kind: PlaceholderKind::LiteralAuto,
        text: "literalAuto",
        auto: true,
        editable: true,
        phase: Phase::Creation,
        behavior: Substitution::Inlined,
        supported: true,
    },
    Row {
        kind: PlaceholderKind::Config,
        text: "config",
        auto: false,
        editable: true,
        phase: Phase::Execution,
        behavior: Substitution::Retained,
        supported: false,
    },
    Row {
        kind: PlaceholderKind::ConfigAuto,
        text: "configAuto",
        auto: true,
        editable: true,
        phase: Phase::Execution,
        behavior: Substitution::Retained,
        supported: false,
    },
    Row {
        kind: PlaceholderKind::Secret,
        text: "secret",
        auto: false,
        editable: true,
        phase: Phase::Execution,
        behavior: Substitution::Retained,
        supported: true,
    },
    Row {
        kind: PlaceholderKind::SecretAuto,
        text: "secretAuto",
        auto: true,
        editable: true,
        phase: Phase::Execution,
        behavior: Substitution::Retained,
        supported: true,
    },
    Row {
        kind: PlaceholderKind::System,
        text: "system",
        auto: false,
        editable: false,
        phase: Phase::Creation,
        behavior: Substitution::Fixed,
        supported: true,
    },
    Row {
        kind: PlaceholderKind::SystemAuto,
        text: "systemAuto",
        auto: true,
        editable: false,
        phase: Phase::Creation,
        behavior: Substitution::Fixed,
        supported: true,
    },
    Row {
        kind: PlaceholderKind::Runtime,
        text: "runtime",
        auto: false,
        editable: false,
        phase: Phase::Execution,
        behavior: Substitution::Fixed,
        supported: false,
    },
    Row {
        kind: PlaceholderKind::RuntimeAuto,
        text: "runtimeAuto",
        auto: true,
        editable: false,
        phase: Phase::Execution,
        behavior: Substitution::Fixed,
        supported: false,
    },
    Row {
        kind: PlaceholderKind::Env,
        text: "env",
        auto: false,
        editable: false,
        phase: Phase::Execution,
        behavior: Substitution::Fixed,
        supported: true,
    },
    Row {
        kind: PlaceholderKind::Var,
        text: "var",
        auto: false,
        editable: false,
        phase: Phase::Creation,
        behavior: Substitution::Fixed,
        supported: false,
    },
];

#[test]
fn matrix_matches_taxonomy() {
    for row in MATRIX {
        assert_eq!(row.kind.as_str(), row.text, "as_str for {}", row.text);
        assert_eq!(row.kind.is_auto(), row.auto, "is_auto for {}", row.text);
        assert_eq!(
            row.kind.is_editable(),
            row.editable,
            "is_editable for {}",
            row.text
        );
        assert_eq!(row.kind.phase(), row.phase, "phase for {}", row.text);
        assert_eq!(
            row.kind.behavior(),
            row.behavior,
            "behavior for {}",
            row.text
        );
        assert_eq!(
            row.kind.is_supported(),
            row.supported,
            "is_supported for {}",
            row.text
        );
    }
}

#[test]
fn requires_bare_token_iff_retained() {
    for row in MATRIX {
        let expected = matches!(row.behavior, Substitution::Retained);
        assert_eq!(
            row.kind.requires_bare_token(),
            expected,
            "requires_bare_token for {}",
            row.text
        );
    }
}

#[test]
fn from_str_round_trips_every_kind() {
    for row in MATRIX {
        let parsed: PlaceholderKind = row.text.parse().expect("parse");
        assert_eq!(parsed, row.kind, "parse for {}", row.text);
    }
}

#[test]
fn from_str_rejects_unknown() {
    assert!("".parse::<PlaceholderKind>().is_err());
    assert!("bogus".parse::<PlaceholderKind>().is_err());
    assert!("Literal".parse::<PlaceholderKind>().is_err());
    assert!("literalauto".parse::<PlaceholderKind>().is_err());
}

#[test]
fn supported_lists_only_supported_kinds() {
    let listed = PlaceholderKind::supported();
    for row in MATRIX {
        let present = listed.split(", ").any(|entry| entry == row.text);
        assert_eq!(present, row.supported, "supported() lists {}", row.text);
    }
}
