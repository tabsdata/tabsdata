//
// Copyright 2026 Tabsdata Inc.
//

use indoc::indoc;

use super::*;

#[test]
fn the_notice_follows_the_copyright_header() {
    let content = indoc! {"
        #
        # Copyright 2026 Tabsdata Inc.
        #

        polars==1.0.0
        numpy==2.0.0
    "};
    let written = discouraged(content);
    let (header, rest) = written.split_once("\n\n").expect("no header break");
    assert_eq!(
        header,
        indoc! {"
        #
        # Copyright 2026 Tabsdata Inc.
        #"}
    );
    assert!(rest.starts_with("# Editing this file is highly discouraged."));
    assert!(written.ends_with(indoc! {"
        polars==1.0.0
        numpy==2.0.0
    "}));
}

#[test]
fn the_requirements_survive_the_notice() {
    let content = indoc! {"
        #
        # Copyright 2026 Tabsdata Inc.
        #

        polars==1.0.0
    "};
    let written = discouraged(content);
    let requirements: Vec<&str> = written
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
        .collect();
    assert_eq!(requirements, ["polars==1.0.0"]);
}
