//
// Copyright 2026 Tabsdata Inc.
//

use chrono::NaiveDate;

use super::*;

fn stamp(windows: bool, compact: Option<&str>) -> String {
    NaiveDate::from_ymd_opt(2026, 10, 30)
        .unwrap()
        .and_hms_milli_opt(6, 7, 41, 515)
        .unwrap()
        .format(trash_timestamp_format(windows, compact))
        .to_string()
}

#[test]
fn trash_stamps_are_dotted_off_windows_unless_compact() {
    for compact in [None, Some(""), Some("false")] {
        assert_eq!(stamp(false, compact), "2026.10.30.06.07.41.515");
    }
}

#[test]
fn trash_stamps_have_no_separators_on_windows_or_when_compact() {
    for (windows, compact) in [
        (true, None),
        (true, Some("false")),
        (false, Some("true")),
        (false, Some(" TRUE ")),
    ] {
        assert_eq!(stamp(windows, compact), "20261030060741515");
    }
}
