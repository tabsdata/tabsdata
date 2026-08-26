//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[test]
fn validate_instance_name_accepts_plain_alphanumeric() {
    assert!(validate_instance_name("tabsdata").is_ok());
    assert!(validate_instance_name("instance123").is_ok());
    assert!(validate_instance_name("ABC").is_ok());
}

#[test]
fn validate_instance_name_accepts_dashes_dots_and_underscores_in_middle() {
    assert!(validate_instance_name("my-instance").is_ok());
    assert!(validate_instance_name("my.instance").is_ok());
    assert!(validate_instance_name("my_instance").is_ok());
    assert!(validate_instance_name("a.b-c_d").is_ok());
}

#[test]
fn validate_instance_name_accepts_unicode_letters() {
    assert!(validate_instance_name("café").is_ok());
    assert!(validate_instance_name("instância").is_ok());
}

#[test]
fn validate_instance_name_accepts_max_length() {
    let name = "a".repeat(MAX_INSTANCE_NAME_LENGTH);
    assert!(validate_instance_name(&name).is_ok());
}

#[test]
fn validate_instance_name_rejects_empty() {
    let err = validate_instance_name("").unwrap_err();
    assert!(err.contains("cannot be empty"));
}

#[test]
fn validate_instance_name_rejects_over_max_length() {
    let name = "a".repeat(MAX_INSTANCE_NAME_LENGTH + 1);
    let err = validate_instance_name(&name).unwrap_err();
    assert!(err.contains("too long"));
}

#[test]
fn validate_instance_name_rejects_each_forbidden_character() {
    for forbidden in FORBIDDEN_CHARACTERS {
        let name = format!("inst{forbidden}name");
        let err =
            validate_instance_name(&name).expect_err("expected error for forbidden character");
        assert!(
            err.contains("reserved character"),
            "missing 'reserved character' in error for '{forbidden}': {err}"
        );
        assert!(
            err.contains(&forbidden.to_string()),
            "error did not mention forbidden char '{forbidden}': {err}"
        );
    }
}

#[test]
fn validate_instance_name_rejects_control_characters() {
    let err = validate_instance_name("abc\x07def").unwrap_err();
    assert!(err.contains("control character"));
    assert!(err.contains("U+0007"));
}

#[test]
fn validate_instance_name_rejects_null_byte_as_control() {
    let err = validate_instance_name("abc\0def").unwrap_err();
    assert!(err.contains("control character"));
}

#[test]
fn validate_instance_name_rejects_trailing_space() {
    let err = validate_instance_name("instance ").unwrap_err();
    assert!(err.contains("space or a dot"));
}

#[test]
fn validate_instance_name_rejects_trailing_dot() {
    let err = validate_instance_name("instance.").unwrap_err();
    assert!(err.contains("space or a dot"));
}

#[test]
fn validate_instance_name_accepts_leading_dot_or_space() {
    // The validator only restricts trailing space/dot, not leading.
    assert!(validate_instance_name(".hidden").is_ok());
}

#[test]
fn validate_instance_name_rejects_reserved_windows_names() {
    for reserved in RESERVED_WINDOWS_NAMES {
        let err = validate_instance_name(reserved).unwrap_err();
        assert!(
            err.contains("reserved Windows name"),
            "missing 'reserved Windows name' for '{reserved}': {err}"
        );
    }
}

#[test]
fn validate_instance_name_reserved_check_is_case_insensitive() {
    assert!(validate_instance_name("con").is_err());
    assert!(validate_instance_name("Con").is_err());
    assert!(validate_instance_name("cOn").is_err());
    assert!(validate_instance_name("LPT3").is_err());
    assert!(validate_instance_name("lpt3").is_err());
}

#[test]
fn validate_instance_name_reserved_check_applies_to_stem_before_first_dot() {
    let err = validate_instance_name("CON.txt").unwrap_err();
    assert!(err.contains("reserved Windows name"));
    let err = validate_instance_name("nul.log.gz").unwrap_err();
    assert!(err.contains("reserved Windows name"));
}

#[test]
fn validate_instance_name_does_not_falsely_reject_similar_names() {
    assert!(validate_instance_name("CONFIG").is_ok());
    assert!(validate_instance_name("PRINTER").is_ok());
    assert!(validate_instance_name("COM10").is_ok());
    assert!(validate_instance_name("COM").is_ok());
    assert!(validate_instance_name("LPT").is_ok());
}

#[test]
fn parse_yes_no_accepts_y_and_yes() {
    assert_eq!(parse_yes_no("y"), Some(true));
    assert_eq!(parse_yes_no("yes"), Some(true));
}

#[test]
fn parse_yes_no_accepts_n_and_no() {
    assert_eq!(parse_yes_no("n"), Some(false));
    assert_eq!(parse_yes_no("no"), Some(false));
}

#[test]
fn parse_yes_no_is_case_insensitive() {
    assert_eq!(parse_yes_no("Y"), Some(true));
    assert_eq!(parse_yes_no("YES"), Some(true));
    assert_eq!(parse_yes_no("Yes"), Some(true));
    assert_eq!(parse_yes_no("N"), Some(false));
    assert_eq!(parse_yes_no("NO"), Some(false));
    assert_eq!(parse_yes_no("No"), Some(false));
    assert_eq!(parse_yes_no("nO"), Some(false));
}

#[test]
fn parse_yes_no_rejects_other_strings() {
    assert_eq!(parse_yes_no(""), None);
    assert_eq!(parse_yes_no("yep"), None);
    assert_eq!(parse_yes_no("nope"), None);
    assert_eq!(parse_yes_no("true"), None);
    assert_eq!(parse_yes_no("1"), None);
    assert_eq!(parse_yes_no(" y "), None);
}

#[test]
fn confirm_short_circuits_when_skip_is_set() {
    assert!(confirm(true, "should not prompt").unwrap());
}
#[test]
fn skipping_bypasses_the_terminal_requirement() {
    assert!(confirm(true, "should not prompt").unwrap());
    assert!(confirm_name(true, "an-instance").unwrap());
    assert!(confirm_at_leisure(true, "should not prompt").unwrap());
}
