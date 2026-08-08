//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[test]
fn the_notice_follows_the_copyright_header() {
    let content = "#\n# Copyright 2026 Tabsdata Inc.\n#\n\npolars==1.0.0\nnumpy==2.0.0\n";
    let written = discouraged(content);
    let (header, rest) = written.split_once("\n\n").expect("no header break");
    assert_eq!(header, "#\n# Copyright 2026 Tabsdata Inc.\n#");
    assert!(rest.starts_with("# Editing this file is highly discouraged."));
    assert!(written.ends_with("polars==1.0.0\nnumpy==2.0.0\n"));
}

#[test]
fn the_requirements_survive_the_notice() {
    let content = "#\n# Copyright 2026 Tabsdata Inc.\n#\n\npolars==1.0.0\n";
    let written = discouraged(content);
    let requirements: Vec<&str> = written
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
        .collect();
    assert_eq!(requirements, ["polars==1.0.0"]);
}
